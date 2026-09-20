package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/source"
)

func recoveryCfg(dir string, every int64) Config {
	cfg := tumblingCfg(0, event.LateInclude, 40*time.Second)
	cfg.CheckpointDir = dir
	cfg.CheckpointEvery = every
	return cfg
}

// 中断 -> 重启续传：已确认输出不重复、未完成窗口不丢失、结果与一次跑完一致。
func TestRestartAfterInterruption(t *testing.T) {
	events := []event.Event{
		eid("a", "k", 1, 10),
		eid("b", "k", 12, 5),
		eid("advance1", "k", 25, 0), // 触发 [0,10)
		eid("c", "k", 35, 7),
		eid("advance2", "k", 45, 0), // 触发 [10,20)、[30,40)
		eid("d", "k", 2, 3),         // 迟到修正 [0,10)（宽限 40s 内）
	}

	// 参考：一次跑完的完整输出（与中断/恢复使用完全相同的屏障周期）。
	ref := quietEngine(t, recoveryCfg("", 3))
	refDst := sink.NewMemory()
	if err := ref.Run(context.Background(), source.NewMemory(events), refDst); err != nil {
		t.Fatal(err)
	}
	refOut := refDst.Outputs()

	dir := t.TempDir()
	// 第一次运行：源投递 3 条后中断（此时应已发出 [0,10) 首版）。
	e1 := quietEngine(t, recoveryCfg(dir, 3)) // 中断点恰为屏障边界
	src1 := source.NewMemoryWithFailure(events, 3)
	dst1 := sink.NewMemory()
	err := e1.Run(context.Background(), src1, dst1)
	if !errors.Is(err, source.ErrInterrupted) {
		t.Fatalf("want ErrInterrupted, got %v", err)
	}
	firstOutputs := len(dst1.Outputs())
	if firstOutputs == 0 {
		t.Fatal("expected some output before interruption")
	}

	// 第二次运行：全新引擎 + 同一检查点目录 + 同一源（从偏移续传）。
	e2 := quietEngine(t, recoveryCfg(dir, 3))
	dst2 := sink.NewMemory()
	// 预置 sink 为"已确认"前次输出：模拟下游持久化输出。
	for _, o := range dst1.Outputs() {
		if err := dst2.Deliver(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := e2.Run(context.Background(), source.NewMemory(events), dst2); err != nil {
		t.Fatalf("resume run: %v", err)
	}

	// 恢复后的完整输出序列必须与参考一致（数量、键、值、类型、修订号、Seq）。
	got := dst2.Outputs()
	if len(got) != len(refOut) {
		t.Fatalf("output count after recovery=%d want %d", len(got), len(refOut))
	}
	for i := range refOut {
		a, b := refOut[i], got[i]
		if a.Seq != b.Seq || a.Key != b.Key || a.Value != b.Value || a.Count != b.Count ||
			a.Kind != b.Kind || a.Revision != b.Revision || !a.Window.Start.Equal(b.Window.Start) {
			t.Fatalf("output %d mismatch:\n ref=%+v\n got=%+v", i, a, b)
		}
	}
	// 水位线恢复后绝不回退：终态一致。
	if !e2.Stats().Watermark.Equal(ref.Stats().Watermark) {
		t.Fatalf("watermark after recovery=%v want %v", e2.Stats().Watermark, ref.Stats().Watermark)
	}
}

// 无中断、逐条检查点重放：输出与不检查点完全相同。
func TestCheckpointEveryRecordNoChange(t *testing.T) {
	events := []event.Event{
		eid("a", "k", 1, 1), eid("b", "k", 11, 2), eid("c", "k", 21, 3),
		eid("d", "j", 1, 9), eid("e", "j", 11, 8),
	}
	for _, n := range []int64{1, 2, 100} {
		cfg := recoveryCfg(t.TempDir(), n)
		e := quietEngine(t, cfg)
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		out := dst.Outputs()
		if len(out) != 5 {
			t.Fatalf("every=%d outputs=%d want 5", n, len(out))
		}
	}
}

// 检查点损坏：引擎必须报 ErrCorrupt，而不是带着猜测状态启动。
func TestCorruptCheckpointRejected(t *testing.T) {
	dir := t.TempDir()
	cfg := recoveryCfg(dir, 1)
	e1 := quietEngine(t, cfg)
	err := e1.Run(context.Background(), source.NewMemory([]event.Event{eid("a", "k", 1, 1)}), sink.NewMemory())
	if err != nil {
		t.Fatal(err)
	}
	// 破坏检查点。
	p := filepath.Join(dir, "chk.json")
	b, _ := os.ReadFile(p)
	os.WriteFile(p, b[:len(b)/3], 0o644)

	e2 := quietEngine(t, recoveryCfg(dir, 3))
	err = e2.Run(context.Background(), source.NewMemory([]event.Event{eid("a", "k", 1, 1)}), sink.NewMemory())
	if !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

// 恢复后已确认输出不重复发出（FileSink 持久化 + Seq 幂等）。
func TestFileSinkNoDuplicateAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "out.jsonl")
	events := []event.Event{eid("a", "k", 1, 10), eid("b", "k", 25, 0)}

	runOnce := func(n int64) {
		cfg := recoveryCfg(filepath.Join(dir, "chk"), n)
		e := quietEngine(t, cfg)
		dst, err := sink.NewFile(outPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		if err := dst.Close(); err != nil {
			t.Fatal(err)
		}
	}
	runOnce(1)
	n1 := countLines(t, outPath)
	if n1 < 1 {
		t.Fatalf("expected outputs, got %d", n1)
	}

	// 再次全新启动并从检查点恢复：不应追加任何重复输出。
	runOnce(1)
	n2 := countLines(t, outPath)
	if n2 != n1 {
		t.Fatalf("duplicate outputs after restore: %d -> %d", n1, n2)
	}
}

func TestWatermarkNeverRegressOnRestore(t *testing.T) {
	dir := t.TempDir()
	cfg := tumblingCfg(0, event.LateDrop, 0)
	cfg.CheckpointDir = dir
	cfg.CheckpointEvery = 1
	e1 := quietEngine(t, cfg)
	if err := e1.Run(context.Background(),
		source.NewMemory([]event.Event{eid("late-ts", "k", 1, 1), eid("hi", "k", 100, 1)}),
		sink.NewMemory()); err != nil {
		t.Fatal(err)
	}
	savedWm := e1.Stats().Watermark

	e2 := quietEngine(t, cfg)
	if err := e2.Run(context.Background(),
		source.NewMemory(nil), // 恢复路径：源偏移已到末尾，立即结束
		sink.NewMemory()); err != nil {
		t.Fatal(err)
	}
	if e2.Stats().Watermark.Before(savedWm) {
		t.Fatalf("watermark regressed after restore: %v < %v", e2.Stats().Watermark, savedWm)
	}
}

// countLines 统计 JSONL 输出行数（重复投递检测）。
func countLines(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, c := range b {
		if c == '\n' {
			n++
		}
	}
	return n
}
