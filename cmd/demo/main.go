// Command demo 使用内存事件源与文件输出端演示：
//  1. 乱序事件 + 水位线推进触发滚动窗口；
//  2. 水位线之后的迟到事件按 revise 策略产生修正输出；
//  3. 关闭后重启，从检查点恢复水位线与窗口状态，不重复发出已确认输出。
//
// 运行：go run ./cmd/demo [--dir /tmp/swe-demo]
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/engine"
	"github.com/lechandonga/stream-window-engine-go/event"
	"github.com/lechandonga/stream-window-engine-go/sink"
)

func main() {
	dir := flag.String("dir", ".swe-demo", "checkpoint/spill/sink directory")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fatal(err)
	}

	if err := runOnce(*dir, logger, false); err != nil {
		fatal(err)
	}
	fmt.Println("--- 重启，从检查点恢复 ---")
	if err := runOnce(*dir, logger, true); err != nil {
		fatal(err)
	}

	fmt.Println("最终输出文件内容（JSON Lines，Seq 不重复）：")
	data, err := os.ReadFile(*dir + "/out.jsonl")
	if err != nil {
		fatal(err)
	}
	fmt.Print(string(data))
}

func runOnce(dir string, logger *slog.Logger, restart bool) error {
	cfg := engine.DefaultConfig()
	cfg.WindowSize = 10 * time.Second
	cfg.WatermarkLag = time.Second
	cfg.AllowedLateness = 60 * time.Second
	cfg.LateEventPolicy = engine.LateRevise
	cfg.MaxWindows = 100
	cfg.CheckpointDir = dir
	cfg.SpillDir = dir

	s, err := sink.NewFileSink(dir + "/out.jsonl")
	if err != nil {
		return err
	}
	defer s.Close()
	store := checkpoint.NewFileStore(dir)
	e, err := engine.New(cfg, engine.Options{Sink: s, Store: store, Logger: logger})
	if err != nil {
		return err
	}
	if err := e.Restore(); err != nil {
		return err
	}

	base := time.Unix(1_000_000, 0)
	if !restart {
		// 乱序到达：1s、8s，随后 15s（把水位线推过 10s 窗口结束）。
		if _, err := e.ProcessBatch([]event.Event{
			{ID: "e1", Key: "orders", Timestamp: base.Add(1 * time.Second), Value: 10},
			{ID: "e2", Key: "orders", Timestamp: base.Add(8 * time.Second), Value: 5},
		}); err != nil {
			return err
		}
		if _, err := e.ProcessBatch([]event.Event{
			{ID: "e3", Key: "orders", Timestamp: base.Add(15 * time.Second), Value: 2},
		}); err != nil {
			return err
		}
		// 迟到事件（时间 3s，已在水位线之后）：产生修正输出。
		if _, err := e.ProcessBatch([]event.Event{
			{ID: "e4-late", Key: "orders", Timestamp: base.Add(3 * time.Second), Value: 100},
		}); err != nil {
			return err
		}
	} else {
		// 重启后再来一个迟到事件，验证状态恢复后仍可修正且不重复首次输出。
		if _, err := e.ProcessBatch([]event.Event{
			{ID: "e5-late", Key: "orders", Timestamp: base.Add(4 * time.Second), Value: 7},
		}); err != nil {
			return err
		}
	}
	return e.Shutdown()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "fatal:", err)
	os.Exit(1)
}

var _ = context.Background
