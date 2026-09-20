package engine_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/engine"
	"github.com/lechandonga/stream-window-engine-go/event"
	"github.com/lechandonga/stream-window-engine-go/sink"
	"github.com/lechandonga/stream-window-engine-go/source"
)

func newTestEngine(t *testing.T, cfg engine.Config) (*engine.Engine, *sink.MemorySink) {
	t.Helper()
	s := sink.NewMemorySink()
	e, err := engine.New(cfg, engine.Options{Sink: s, Store: checkpoint.NewMemoryStore()})
	if err != nil {
		t.Fatal(err)
	}
	return e, s
}

func TestInvalidTimeRejectAndAssign(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	baseCfg2 := func(reject bool) engine.Config {
		cfg := engine.DefaultConfig()
		cfg.RejectInvalidTime = reject
		cfg.MinEventTime = time.Unix(1000, 0)
		cfg.FutureTolerance = 1 * time.Minute
		return cfg
	}

	// reject 策略：缺失时间 + 过老时间 + 过未来时间都记录并拒绝。
	cfg := baseCfg2(true)
	s := sink.NewMemorySink()
	e, err := engine.New(cfg, engine.Options{
		Sink: s, Store: checkpoint.NewMemoryStore(),
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := e.ProcessBatch([]event.Event{
		{ID: "missing", Key: "k", Value: 1},
		{ID: "tooold", Key: "k", Timestamp: time.Unix(10, 0), Value: 2},
		{ID: "toofar", Key: "k", Timestamp: now.Add(2 * time.Hour), Value: 3},
	})
	if err != nil {
		t.Fatalf("rejected events must be reported, not fatal: %v", err)
	}
	if n != 0 {
		t.Fatalf("accepted=%d want 0", n)
	}
	recs := e.InvalidEvents()
	if len(recs) != 3 {
		t.Fatalf("want 3 invalid records, got %d: %+v", len(recs), recs)
	}
	reasons := map[string]bool{}
	for _, r := range recs {
		reasons[r.Reason] = true
		if r.Policy != "reject" {
			t.Fatalf("policy=%s", r.Policy)
		}
	}
	for _, want := range []string{"missing-event-time", "event-time-before-min-allowed", "event-time-exceeds-future-tolerance"} {
		if !reasons[want] {
			t.Fatalf("missing reason %s in %+v", want, reasons)
		}
	}

	// assign 策略：同样的异常事件被重分配当前时间后继续处理，且仍有记录。
	cfg2 := baseCfg2(false)
	s2 := sink.NewMemorySink()
	e2, err := engine.New(cfg2, engine.Options{
		Sink: s2, Store: checkpoint.NewMemoryStore(),
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err = e2.ProcessBatch([]event.Event{
		{ID: "missing", Key: "k", Value: 1},
		{ID: "tooold", Key: "k", Timestamp: time.Unix(10, 0), Value: 2},
	})
	if err != nil || n != 2 {
		t.Fatalf("assign policy: accepted=%d err=%v", n, err)
	}
	if e2.InvalidEventsCount() != 2 {
		t.Fatalf("assign policy must still record invalid events, got %d", e2.InvalidEventsCount())
	}
}

func TestStateLimitBackpressure(t *testing.T) {
	cfg := engine.DefaultConfig()
	cfg.WindowSize = 1 * time.Second
	cfg.MaxWindows = 2
	cfg.OnOverflow = engine.OverflowBlock
	cfg.WatermarkLag = 0
	e, _ := newTestEngine(t, cfg)
	base := time.Unix(0, 0)
	// 同一批中需要 3 个不同 key 的窗口 -> 超过分片上限 2 -> 背压，且状态不变更。
	_, err := e.ProcessBatch([]event.Event{
		{ID: "a", Key: "A", Timestamp: base, Value: 1},
		{ID: "b", Key: "B", Timestamp: base, Value: 1},
		{ID: "c", Key: "C", Timestamp: base, Value: 1},
	})
	if !errors.Is(err, engine.ErrStateLimit) {
		t.Fatalf("want ErrStateLimit, got %v", err)
	}
}

func TestStateLimitSpillToDisk(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.DefaultConfig()
	cfg.WindowSize = 1 * time.Second
	cfg.MaxWindows = 2
	cfg.OnOverflow = engine.OverflowSpill
	cfg.SpillDir = dir
	cfg.CheckpointDir = dir
	cfg.WatermarkLag = 0
	cfg.AllowedLateness = 3600 * time.Second
	s := sink.NewMemorySink()
	e, err := engine.New(cfg, engine.Options{Sink: s, Store: checkpoint.NewFileStore(dir)})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(0, 0)
	// 第一批建 2 个窗口；第二批推进水位线并再建 2 个 -> 触发驱逐落盘。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "a", Key: "A", Timestamp: base, Value: 11},
		{ID: "b", Key: "B", Timestamp: base, Value: 22},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "c", Key: "C", Timestamp: base.Add(10 * time.Second), Value: 33},
		{ID: "d", Key: "D", Timestamp: base.Add(10 * time.Second), Value: 44},
	}); err != nil {
		t.Fatalf("spill should absorb overflow: %v", err)
	}
	// 迟到事件访问被驱逐的 A 窗口：惰性加载并修订，结果正确。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "late-a", Key: "A", Timestamp: base.Add(100 * time.Millisecond), Value: 100},
	}); err != nil {
		t.Fatal(err)
	}
	var foundA *sink.Record
	for _, r := range s.Records() {
		if r.Key == "A" {
			rr := r
			foundA = &rr
		}
	}
	if foundA == nil || foundA.Value != 111 || !foundA.IsCorrection {
		t.Fatalf("spilled window reload/revise wrong: %+v", s.Records())
	}
}

func TestSourceInterruptionDetected(t *testing.T) {
	cfg := engine.DefaultConfig()
	e, _ := newTestEngine(t, cfg)
	src := source.NewSliceSource([]source.EventBatch{
		{Events: []event.Event{{ID: "a", Key: "k", Timestamp: time.Unix(0, 1), Value: 1}}},
	})
	src.MarkInterrupted()
	err := e.Run(context.Background(), src)
	if !errors.Is(err, engine.ErrSourceFailed) {
		t.Fatalf("want ErrSourceFailed, got %v", err)
	}
	if !strings.Contains(err.Error(), source.ErrSourceInterrupted.Error()) {
		t.Fatalf("error should carry distinguishable reason, got %v", err)
	}
}
