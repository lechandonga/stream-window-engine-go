package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/source"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
)

// 内存上限 + 落盘：大量窗口在流水线中不丢数据、不重复、结果正确。
func TestSpillUnderMemoryLimit(t *testing.T) {
	dir := t.TempDir()
	cfg := tumblingCfg(0, event.LateDrop, 0)
	cfg.CheckpointEvery = 1
	cfg.State = state.Config{
		MaxActiveKeys: 4,
		Overflow:      state.OverflowSpill,
		SpillDir:      dir,
	}
	// 构造 5 个分组 × 6 个窗口 = 30 个窗口，远超内存上限 4。
	var events []event.Event
	// 按时间戳交错（而非按分组聚集），保证全局水位线不会让某组整体迟到。
	for s := int64(0); s < 60; s += 10 {
		for g := 0; g < 5; g++ {
			events = append(events, eid(
				"e-"+itoa(int64(g))+"-"+itoa(s),
				"g"+itoa(int64(g)), s, s+1))
		}
	}
	e := quietEngine(t, cfg)
	dst := sink.NewMemory()
	if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
		t.Fatal(err)
	}
	out := dst.Outputs()
	if len(out) != 30 {
		t.Fatalf("windows fired=%d want 30 (spill must not lose windows)", len(out))
	}
	// 每个窗口值 = 起始秒 + 1，count=1；验证落盘读回的数据正确。
	for _, o := range out {
		startSec := o.Window.Start.Sub(testBase).Nanoseconds() / int64(time.Second)
		if o.Count != 1 || o.Value != startSec+1 {
			t.Fatalf("window %s value=%d count=%d want %d/1",
				o.Window.Start.Sub(testBase), o.Value, o.Count, startSec+1)
		}
	}
	if e.Stats().Spills == 0 {
		t.Fatal("expected spill activity under tight memory limit")
	}
}

// 背压策略：上限极小时仍能完成处理（窗口在 flush 后被关闭清理释放空间）。
func TestBackpressureCompletes(t *testing.T) {
	cfg := tumblingCfg(0, event.LateDrop, 0)
	cfg.CheckpointEvery = 1
	cfg.State = state.Config{MaxActiveKeys: 2, Overflow: state.OverflowBlock}
	var events []event.Event
	for s := int64(0); s < 40; s++ {
		events = append(events, eid("e"+itoa(s), "k", s, 1))
	}
	e := quietEngine(t, cfg)
	dst := sink.NewMemory()
	done := make(chan error, 1)
	go func() { done <- e.Run(context.Background(), source.NewMemory(events), dst) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run under backpressure: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("engine deadlocked under memory cap with backpressure")
	}
	if len(dst.Outputs()) != 4 {
		t.Fatalf("windows=%d want 4", len(dst.Outputs()))
	}
}

// 异常时间的赋值策略也要留下记录且结果可解释。
func TestInvalidAssignPolicy(t *testing.T) {
	for _, p := range []event.InvalidTimePolicy{event.InvalidAssignWatermark, event.InvalidAssignProcessingTime} {
		cfg := tumblingCfg(10*time.Second, event.LateDrop, 0)
		cfg.Invalid = p
		cfg.CheckpointEvery = 1
		// 处理时钟与数据时间接近，避免"赋值为处理时间"的事件跑到遥远未来，
		// 进而让真实事件反成迟到（这里只验证异常事件被记录且被计数）。
		e, err := New(cfg,
			WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
			WithClock(func() time.Time { return testBase.Add(4 * time.Second) }))
		if err != nil {
			t.Fatal(err)
		}
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory([]event.Event{
			{ID: "miss", Key: "k", Value: 9},
			eid("ok", "k", 5, 1),
		}), dst); err != nil {
			t.Fatal(err)
		}
		if len(dst.Sides()) != 1 || dst.Sides()[0].Decision.Reason != event.ReasonMissingTime {
			t.Fatalf("policy %v: missing-time must be recorded, got %+v", p, dst.Sides())
		}
		// 被赋值时间的事件必须被计入某个窗口（未静默丢弃）。
		total := int64(0)
		for _, o := range dst.Outputs() {
			total += o.Count
		}
		if total != 2 {
			t.Fatalf("policy %v: assigned event must be counted, total=%d", p, total)
		}
	}
}

// 落盘状态 + 检查点 + 重启：被淘汰到磁盘的窗口在恢复后仍正确触发，结果不丢。
func TestSpillSurvivesRestart(t *testing.T) {
	base := t.TempDir()
	events := make([]event.Event, 0, 40)
	for s := int64(0); s < 40; s++ {
		events = append(events, eid("e"+itoa(s), "k", s, 1))
	}
	cfg := func() Config {
		c := tumblingCfg(0, event.LateDrop, 0)
		c.CheckpointEvery = 8
		c.State = state.Config{
			MaxActiveKeys: 2,
			Overflow:      state.OverflowSpill,
			SpillDir:      filepath.Join(base, "spill"),
		}
		c.CheckpointDir = filepath.Join(base, "chk")
		return c
	}

	// 第一次：源投递 8 条（一个屏障）后中断。
	e1, err := New(cfg(),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		WithClock(func() time.Time { return testBase.Add(1000 * time.Second) }))
	if err != nil {
		t.Fatal(err)
	}
	dst1 := sink.NewMemory()
	err = e1.Run(context.Background(), source.NewMemoryWithFailure(events, 8), dst1)
	if !errors.Is(err, source.ErrInterrupted) {
		t.Fatalf("want interruption, got %v", err)
	}
	firedFirst := len(dst1.Outputs())

	// 恢复并跑完剩余事件（全新引擎实例、同一检查点与落盘目录）。
	e2, err := New(cfg(),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		WithClock(func() time.Time { return testBase.Add(1000 * time.Second) }))
	if err != nil {
		t.Fatal(err)
	}
	dst2 := sink.NewMemory()
	for _, o := range dst1.Outputs() {
		_ = dst2.Deliver(o)
	}
	if err := e2.Run(context.Background(), source.NewMemory(events), dst2); err != nil {
		t.Fatal(err)
	}
	out := dst2.Outputs()
	// 40 个事件 / 10s 窗口 = 4 个窗口，跨重启全部触发且无重复 Seq。
	if len(out) != 4 {
		t.Fatalf("windows=%d want 4 (first run fired %d)", len(out), firedFirst)
	}
	seenSeq := map[int64]bool{}
	var total int64
	for _, o := range out {
		if seenSeq[o.Seq] {
			t.Fatalf("duplicate seq %d after restart with spill", o.Seq)
		}
		seenSeq[o.Seq] = true
		total += o.Count
	}
	if total != 40 {
		t.Fatalf("total event count=%d want 40 (spill must not lose events)", total)
	}
}
