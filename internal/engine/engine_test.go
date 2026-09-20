package engine

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/source"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

func quietEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	e, err := New(cfg, WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		WithClock(func() time.Time { return time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC) }))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func tumblingCfg(lag time.Duration, late event.LatePolicy, allowed time.Duration) Config {
	return Config{
		Window:          window.Spec{Kind: window.Tumbling, Size: 10 * time.Second},
		Aggregator:      "sum_count",
		Lag:             lag,
		Late:            late,
		AllowedLateness: allowed,
		Invalid:         event.InvalidReject,
		FutureBound:     24 * time.Hour,
		State:           state.Config{},
	}
}

var testBase = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

func eid(id string, key string, tsSec, v int64) event.Event {
	return event.Event{ID: id, Key: key, Ts: testBase.Add(time.Duration(tsSec) * time.Second), Value: v}
}

// 乱序到达下，窗口归属与结果必须确定。
func TestOutOfOrderTumbling(t *testing.T) {
	cfg := tumblingCfg(20*time.Second, event.LateDrop, 0)
	e := quietEngine(t, cfg)
	src := source.NewMemory([]event.Event{
		eid("a", "k", 12, 2), // 窗口 [10,20)
		eid("b", "k", 1, 5),  // 窗口 [0,10)
		eid("c", "k", 15, 3), // 窗口 [10,20)
		eid("d", "k", 5, 1),  // 窗口 [0,10)
	})
	dst := sink.NewMemory()
	if err := e.Run(context.Background(), src, dst); err != nil {
		t.Fatal(err)
	}
	out := dst.Outputs()
	if len(out) != 2 {
		t.Fatalf("outputs=%d want 2: %+v", len(out), out)
	}
	if out[0].Key != "k" || out[0].Value != 6 || out[0].Count != 2 {
		t.Fatalf("window[0] wrong: %+v", out[0])
	}
	if out[1].Value != 5 || out[1].Count != 2 {
		t.Fatalf("window[1] wrong: %+v", out[1])
	}
	for _, o := range out {
		if o.Kind != aggregate.Insert || o.Revision != 1 {
			t.Fatalf("all outputs should be INSERT rev1, got %s rev%d", o.Kind, o.Revision)
		}
	}
}

// 确定性：重放同一输入必须得到相同输出序列。
func TestReplayDeterminism(t *testing.T) {
	events := []event.Event{
		eid("a", "k1", 12, 2), eid("b", "k2", 3, 9), eid("c", "k1", 1, 5),
		eid("d", "k2", 25, 1), eid("e", "k1", 21, 7), eid("f", "k2", 8, 4),
	}
	run := func(conc int) []aggregate.Output {
		cfg := tumblingCfg(20*time.Second, event.LateDrop, 0)
		cfg.Concurrency = conc
		e := quietEngine(t, cfg)
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		return dst.Outputs()
	}
	first := run(1)
	second := run(8)
	if len(first) != len(second) {
		t.Fatalf("len %d vs %d", len(first), len(second))
	}
	for i := range first {
		a, b := first[i], second[i]
		if a.Seq != b.Seq || a.Key != b.Key || a.Value != b.Value ||
			a.Window.Start != b.Window.Start || a.Kind != b.Kind || a.Revision != b.Revision {
			t.Fatalf("output %d differs:\n %+v\n %+v", i, a, b)
		}
	}
}

// 迟到事件：LateInclude + 宽限期内产生可区分的 CORRECTION；
// 宽限期外丢弃并留旁路记录；LateDrop 直接旁路。
func TestLatePolicies(t *testing.T) {
	events := []event.Event{
		eid("early", "k", 1, 10),
		eid("advance", "k", 25, 0), // 水位线推进到 25
		eid("late-in", "k", 5, 7),  // 迟到，窗口 [0,10)
	}

	t.Run("include_within_lateness", func(t *testing.T) {
		cfg := tumblingCfg(0, event.LateInclude, 30*time.Second)
		cfg.CheckpointEvery, cfg.CheckpointDir = 1, t.TempDir()
		e := quietEngine(t, cfg)
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		var w0 []aggregate.Output
		for _, o := range dst.Outputs() {
			if o.Window.Start.Equal(testBase) {
				w0 = append(w0, o)
			}
		}
		if len(w0) != 2 || w0[0].Kind != aggregate.Insert || w0[1].Kind != aggregate.Correction {
			t.Fatalf("expected INSERT then CORRECTION, got %+v", w0)
		}
		if w0[1].Value != 17 || w0[1].Revision != 2 || !w0[1].Late {
			t.Fatalf("correction wrong: %+v", w0[1])
		}
		if w0[0].Seq == w0[1].Seq {
			t.Fatal("seq must differ between insert and correction")
		}
	})

	t.Run("drop_beyond_lateness", func(t *testing.T) {
		cfg := tumblingCfg(0, event.LateInclude, 5*time.Second) // wm25 > End10+5=15
		cfg.CheckpointEvery, cfg.CheckpointDir = 1, t.TempDir()
		e := quietEngine(t, cfg)
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		var w0 []aggregate.Output
		for _, o := range dst.Outputs() {
			if o.Window.Start.Equal(testBase) {
				w0 = append(w0, o)
			}
		}
		if len(w0) != 1 || w0[0].Value != 10 {
			t.Fatalf("late event beyond lateness must not correct, got %+v", w0)
		}
		sides := dst.Sides()
		found := false
		for _, s := range sides {
			if s.Decision.EventID == "late-in" && s.Decision.Reason == event.ReasonLateDropped {
				found = true
			}
		}
		if !found {
			t.Fatalf("dropped late event must be recorded, sides=%+v", sides)
		}
	})

	t.Run("drop_policy_side_output", func(t *testing.T) {
		cfg := tumblingCfg(0, event.LateDrop, 0)
		cfg.CheckpointEvery, cfg.CheckpointDir = 1, t.TempDir()
		e := quietEngine(t, cfg)
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, s := range dst.Sides() {
			if s.Decision.EventID == "late-in" {
				found = true
			}
		}
		if !found {
			t.Fatal("LateDrop must leave a side record")
		}
	})

	t.Run("side_output_policy", func(t *testing.T) {
		cfg := tumblingCfg(0, event.LateSideOutput, 0)
		cfg.CheckpointEvery, cfg.CheckpointDir = 1, t.TempDir()
		e := quietEngine(t, cfg)
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, s := range dst.Sides() {
			if s.Decision.Reason == event.ReasonLateSideOutput && s.Decision.EventID == "late-in" {
				found = true
			}
		}
		if !found {
			t.Fatal("LateSideOutput must produce a side-output record")
		}
	})
}

// 重复投递：相同事件 ID 二次进入不得重复计数或重复修正。
func TestDuplicateRedelivery(t *testing.T) {
	cfg := tumblingCfg(0, event.LateInclude, 30*time.Second)
	cfg.CheckpointEvery, cfg.CheckpointDir = 1, t.TempDir()
	e := quietEngine(t, cfg)
	events := []event.Event{
		eid("a", "k", 1, 10),
		eid("a-dup", "k", 1, 10), // 不同 ID 的同值事件不是重复，用于对照
	}
	_ = events
	// 真重复：相同 ID 再次投递（包括迟到路径）。
	src := source.NewMemory([]event.Event{
		eid("a", "k", 1, 10),
		eid("b", "k", 25, 0),
		eid("a", "k", 1, 10), // 迟到的重复投递
		eid("c", "k", 3, 4),  // 真正的迟到新事件
	})
	dst := sink.NewMemory()
	if err := e.Run(context.Background(), src, dst); err != nil {
		t.Fatal(err)
	}
	var w0 []aggregate.Output
	for _, o := range dst.Outputs() {
		if o.Window.Start.Equal(testBase) {
			w0 = append(w0, o)
		}
	}
	if len(w0) != 2 {
		t.Fatalf("want INSERT + one CORRECTION, got %+v", w0)
	}
	if w0[1].Value != 14 || w0[1].Count != 2 {
		t.Fatalf("duplicate must not double count, correction=%+v", w0[1])
	}
	st := e.Stats()
	if st.Duplicates != 1 {
		t.Fatalf("duplicates stat=%d want 1", st.Duplicates)
	}
}

// 异常时间事件不得被静默丢弃。
func TestInvalidTimeRecorded(t *testing.T) {
	cfg := tumblingCfg(20*time.Second, event.LateDrop, 0)
	cfg.Invalid = event.InvalidReject
	e := quietEngine(t, cfg)
	src := source.NewMemory([]event.Event{
		{ID: "missing", Key: "k", Value: 1},                      // 无时间
		{ID: "ancient", Key: "k", Ts: time.Unix(1, 0), Value: 1}, // 1970 年，异常           // 2000 年前 -> 异常（ts 早于 EarliestPlausibleTime）
		eid("ok", "k", 1, 2),
	})
	dst := sink.NewMemory()
	if err := e.Run(context.Background(), src, dst); err != nil {
		t.Fatal(err)
	}
	reasons := map[string]event.Reason{}
	for _, s := range dst.Sides() {
		reasons[s.Decision.EventID] = s.Decision.Reason
	}
	if reasons["missing"] != event.ReasonMissingTime {
		t.Fatalf("missing-time not recorded: %v", reasons)
	}
	if reasons["ancient"] != event.ReasonTimeOutOfRange {
		t.Fatalf("ancient event not recorded: %v", reasons)
	}
	var w0 []aggregate.Output
	for _, o := range dst.Outputs() {
		if o.Window.Start.Equal(testBase) {
			w0 = append(w0, o)
		}
	}
	if len(w0) != 1 || w0[0].Value != 2 {
		t.Fatalf("rejected events must not aggregate, got %+v", w0)
	}
}
