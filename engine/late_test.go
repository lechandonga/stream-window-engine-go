package engine_test

import (
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/engine"
	"github.com/lechandonga/stream-window-engine-go/event"
	"github.com/lechandonga/stream-window-engine-go/sink"
)

// setupLate：先让窗口 [0,10s) 以 value=3 首次输出（水位线推进到 12s）。
func setupLate(t *testing.T, policy engine.LatePolicy, allowed time.Duration) (*engine.Engine, *sink.MemorySink) {
	t.Helper()
	cfg := baseCfg()
	cfg.LateEventPolicy = policy
	cfg.AllowedLateness = allowed
	cfg.WatermarkLag = 0
	e, s, _ := mkEngine(t, cfg)
	base := time.Unix(0, 0)
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "a", Key: "k", Timestamp: base.Add(1 * time.Second), Value: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "wm1", Key: "k", Timestamp: base.Add(12 * time.Second), Value: 0},
	}); err != nil {
		t.Fatal(err)
	}
	return e, s
}

func TestLateReviseEmitsCorrection(t *testing.T) {
	base := time.Unix(0, 0)
	e, s := setupLate(t, engine.LateRevise, 30*time.Second)
	before := s.Records()
	if len(before) != 1 || before[0].IsCorrection {
		t.Fatalf("precondition: %+v", before)
	}
	// 迟到事件（时间 2s < 水位线 12s），但仍在 allowed lateness 内。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "late1", Key: "k", Timestamp: base.Add(2 * time.Second), Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	recs := s.Records()
	if len(recs) != 2 {
		t.Fatalf("want correction record, got %+v", recs)
	}
	corr := recs[1]
	if !corr.IsCorrection || corr.Value != 8 || corr.Reason != "late-revision" {
		t.Fatalf("unexpected correction: %+v", corr)
	}
	if e.LateEventsCount() != 1 {
		t.Fatalf("want 1 late record, got %d", e.LateEventsCount())
	}
}

func TestLateDropDoesNotChangeResult(t *testing.T) {
	base := time.Unix(0, 0)
	e, s := setupLate(t, engine.LateDrop, 30*time.Second)
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "late1", Key: "k", Timestamp: base.Add(2 * time.Second), Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	recs := s.Records()
	if len(recs) != 1 {
		t.Fatalf("drop policy must not emit correction, got %+v", recs)
	}
	if recs[0].Value != 3 {
		t.Fatalf("result changed by dropped event: %d", recs[0].Value)
	}
	rec := e.LateEvents()
	if len(rec) != 1 || rec[0].Action != "late-drop" || rec[0].Policy != "drop" {
		t.Fatalf("unexpected late record: %+v", rec)
	}
}

func TestLateRedirectSideOutput(t *testing.T) {
	base := time.Unix(0, 0)
	e, s := setupLate(t, engine.LateRedirect, 30*time.Second)
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "late1", Key: "k", Timestamp: base.Add(2 * time.Second), Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if len(s.Records()) != 1 {
		t.Fatalf("redirect must not change main output: %+v", s.Records())
	}
	side := e.SideOutputs()
	if len(side) != 1 || side[0].EventID != "late1" || side[0].Value != 5 {
		t.Fatalf("unexpected side output: %+v", side)
	}
	late := e.LateEvents()
	if len(late) != 1 || late[0].Action != "late-redirect" {
		t.Fatalf("unexpected late record: %+v", late)
	}
}

func TestLateBeyondAllowedLatenessExpired(t *testing.T) {
	base := time.Unix(0, 0)
	// allowed lateness = 0：窗口 fired 后（wm=12 >= end+0=10）迟到事件必须过期丢弃。
	e, s := setupLate(t, engine.LateRevise, 0)
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "late1", Key: "k", Timestamp: base.Add(2 * time.Second), Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if len(s.Records()) != 1 || s.Records()[0].Value != 3 {
		t.Fatalf("expired late event must not revise: %+v", s.Records())
	}
	late := e.LateEvents()
	if len(late) != 1 || late[0].Action != "late-drop-expired" {
		t.Fatalf("want late-drop-expired, got %+v", late)
	}
}

func TestDuplicateDeliveryIdempotent(t *testing.T) {
	base := time.Unix(0, 0)
	e, s := setupLate(t, engine.LateRevise, 30*time.Second)
	ev := []event.Event{{ID: "late1", Key: "k", Timestamp: base.Add(2 * time.Second), Value: 5}}
	if _, err := e.ProcessBatch(ev); err != nil {
		t.Fatal(err)
	}
	// 完全相同的事件（相同 ID）再次投递：不得重复计数、不得重复修正。
	if _, err := e.ProcessBatch(ev); err != nil {
		t.Fatal(err)
	}
	recs := s.Records()
	if len(recs) != 2 {
		t.Fatalf("duplicate delivery produced extra correction: %+v", recs)
	}
	if recs[1].Value != 8 {
		t.Fatalf("value=%d want 8", recs[1].Value)
	}
}

var _ = checkpoint.NewMemoryStore
