// Command demo 以确定性数据演示乱序、迟到、重复投递、异常时间与重启恢复。
//
// 用法：
//
//	go run ./cmd/demo                # 完整跑一遍并打印判定日志
//	go run ./cmd/demo -dir <目录>    # 指定检查点/输出目录
//	go run ./cmd/demo -interrupt     # 源投递若干条后中断（返回非零退出码）
//	再次运行（不带 -interrupt）即从检查点恢复，输出不重复。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/engine"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/source"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

func main() {
	dir := flag.String("dir", filepath.Join(os.TempDir(), "swe-demo"), "checkpoint/output directory")
	interrupt := flag.Bool("interrupt", false, "interrupt the source after a few records")
	conc := flag.Int("concurrency", 4, "group processing concurrency")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	at := func(sec int64) time.Time { return base.Add(time.Duration(sec) * time.Second) }

	events := []event.Event{
		{ID: "e01", Key: "room-A", Ts: at(2), Value: 10},
		{ID: "e02", Key: "room-A", Ts: at(8), Value: 5}, // 乱序但同窗口
		{ID: "e03", Key: "room-B", Ts: at(3), Value: 7},
		{ID: "e04", Key: "room-A", Ts: at(14), Value: 2},
		{ID: "e05", Key: "room-B", Ts: at(25), Value: 0},  // 推进水位线，触发 [0,10)
		{ID: "e06", Key: "room-A", Ts: at(5), Value: 100}, // 迟到：[0,10) 修正
		{ID: "e06", Key: "room-A", Ts: at(5), Value: 100}, // 重复投递：必须忽略
		{ID: "bad-ts", Key: "room-A", Value: 3},           // 缺失时间：记录不静默丢弃
		{ID: "e07", Key: "room-B", Ts: at(33), Value: 4},
	}

	cfg := engine.Config{
		Window:          window.Spec{Kind: window.Tumbling, Size: 10 * time.Second},
		Aggregator:      "sum_count",
		Lag:             2 * time.Second,
		Late:            event.LateInclude,
		AllowedLateness: 30 * time.Second,
		Invalid:         event.InvalidReject,
		FutureBound:     24 * time.Hour,
		State:           state.Config{MaxActiveKeys: 64, Overflow: state.OverflowBlock},
		CheckpointDir:   filepath.Join(*dir, "chk"),
		CheckpointEvery: 1,
		Concurrency:     *conc,
	}

	eng, err := engine.New(cfg,
		engine.WithLogger(logger),
		engine.WithClock(func() time.Time { return at(100000) }),
	)
	if err != nil {
		fail("build engine: %v", err)
	}

	outPath := filepath.Join(*dir, "output.jsonl")
	dst, err := sink.NewFile(outPath)
	if err != nil {
		fail("open sink: %v", err)
	}
	defer dst.Close()

	var src source.Source
	if *interrupt {
		src = source.NewMemoryWithFailure(events, 5)
	} else {
		src = source.NewMemory(events)
	}

	if err := eng.Run(context.Background(), src, dst); err != nil {
		logger.Error("run ended", "error", err)
		fmt.Fprintf(os.Stderr, "\nengine stopped: %v\n已处理的屏障已检查点；修复来源后重新运行即可续传。\n", err)
		os.Exit(2)
	}

	fmt.Println("\n===== 已确认聚合输出（INSERT=首次, CORRECTION=迟到修正）=====")
	for _, o := range dst.Outputs() {
		fmt.Printf("seq=%-3d rev=%d %-10s key=%-7s window=[%s,%s) sum=%d count=%d\n",
			o.Seq, o.Revision, o.Kind, o.Key,
			o.Window.Start.Format("15:04:05"), o.Window.End.Format("15:04:05"),
			o.Value, o.Count)
	}
	if sides := dst.Sides(); len(sides) > 0 {
		fmt.Println("\n===== 旁路记录（迟到/异常，未静默丢弃）=====")
		for _, s := range sides {
			fmt.Printf("event=%-7s key=%-7s reason=%s detail=%s\n",
				s.Decision.EventID, s.Decision.Key, s.Decision.Reason, s.Decision.Detail)
		}
	}
	st := eng.Stats()
	fmt.Printf("\n统计 processed=%d duplicates=%d late=%d rejected=%d checkpoints=%d final-watermark=%s\n",
		st.Processed, st.Duplicates, st.LateHandled, st.Rejected, st.Checkpoints,
		st.Watermark.Format(time.RFC3339))
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
