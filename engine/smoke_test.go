package engine_test

import (
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/engine"
	"github.com/lechandonga/stream-window-engine-go/event"
	"github.com/lechandonga/stream-window-engine-go/sink"
)

func baseCfg() engine.Config {
	cfg := engine.DefaultConfig()
	cfg.WindowSize = 10 * time.Second
	cfg.WatermarkLag = 0
	cfg.AllowedLateness = 10 * time.Second
	return cfg
}

func mkEngine(t *testing.T, cfg engine.Config) (*engine.Engine, *sink.MemorySink, *checkpoint.MemoryStore) {
	t.Helper()
	s := sink.NewMemorySink()
	st := checkpoint.NewMemoryStore()
	e, err := engine.New(cfg, engine.Options{Sink: s, Store: st})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return e, s, st
}

func TestSmokeTumblingFire(t *testing.T) {
	base := time.Unix(0, 0)
	e, s, _ := mkEngine(t, baseCfg())
	// 第一批事件落在窗口 [0,10s)；第二批事件时间 11s 推进水位线 >=10s 触发窗口
	_, err := e.ProcessBatch([]event.Event{
		{ID: "a", Key: "k1", Timestamp: base.Add(1 * time.Second), Value: 3},
		{ID: "b", Key: "k1", Timestamp: base.Add(2 * time.Second), Value: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "c", Key: "k1", Timestamp: base.Add(11 * time.Second), Value: 1},
	}); err != nil {
		t.Fatal(err)
	}
	recs := s.Records()
	if len(recs) != 1 {
		t.Fatalf("want 1 fired record, got %d: %+v", len(recs), recs)
	}
	if recs[0].Value != 7 || recs[0].IsCorrection {
		t.Fatalf("unexpected first record: %+v", recs[0])
	}
	if recs[0].Reason != "window-fire" {
		t.Fatalf("reason=%s", recs[0].Reason)
	}
}
