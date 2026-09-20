package engine_test

import (
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/engine"
	"github.com/lechandonga/stream-window-engine-go/event"
	"github.com/lechandonga/stream-window-engine-go/sink"
)

func TestSpilledWindowSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.DefaultConfig()
	cfg.WindowSize = 1 * time.Second
	cfg.MaxWindows = 2
	cfg.OnOverflow = engine.OverflowSpill
	cfg.SpillDir = dir
	cfg.CheckpointDir = dir
	cfg.WatermarkLag = 0
	cfg.AllowedLateness = 3600 * time.Second

	run := func(sink0 *sink.MemorySink) *engine.Engine {
		e, err := engine.New(cfg, engine.Options{Sink: sink0, Store: checkpoint.NewFileStore(dir)})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	s1 := sink.NewMemorySink()
	e1 := run(s1)
	base := time.Unix(0, 0)
	if _, err := e1.ProcessBatch([]event.Event{
		{ID: "a", Key: "A", Timestamp: base, Value: 11},
		{ID: "b", Key: "B", Timestamp: base, Value: 22},
	}); err != nil {
		t.Fatal(err)
	}
	// 推进水位线触发 A/B（此时它们被驱逐落盘），并做显式检查点。
	if _, err := e1.ProcessBatch([]event.Event{
		{ID: "c", Key: "C", Timestamp: base.Add(10 * time.Second), Value: 33},
		{ID: "d", Key: "D", Timestamp: base.Add(10 * time.Second), Value: 44},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e1.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	firedBefore := len(s1.Records())
	if firedBefore < 2 {
		t.Fatalf("precondition: A/B should have fired, got %+v", s1.Records())
	}

	// 重启。
	s2 := sink.NewMemorySink()
	e2 := run(s2)
	if err := e2.Restore(); err != nil {
		t.Fatal(err)
	}
	// 迟到事件命中已落盘的 A 窗口：必须惰性加载并修正。
	if _, err := e2.ProcessBatch([]event.Event{
		{ID: "late-a", Key: "A", Timestamp: base.Add(100 * time.Millisecond), Value: 100},
	}); err != nil {
		t.Fatal(err)
	}
	var corr *sink.Record
	for i := range s2.Records() {
		r := s2.Records()[i]
		if r.Key == "A" && r.IsCorrection {
			corr = &r
		}
	}
	if corr == nil || corr.Value != 111 {
		t.Fatalf("after restart, spilled late revise missing/wrong: %+v", s2.Records())
	}
}
