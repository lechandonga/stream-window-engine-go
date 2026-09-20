package engine_test

import (
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/engine"
	"github.com/lechandonga/stream-window-engine-go/event"
	"github.com/lechandonga/stream-window-engine-go/sink"
)

func TestSlidingWindowMultiAssignment(t *testing.T) {
	cfg := engine.DefaultConfig()
	cfg.WindowType = engine.WindowSliding
	cfg.WindowSize = 10 * time.Second
	cfg.Slide = 5 * time.Second
	cfg.WatermarkLag = 0
	cfg.AllowedLateness = 0
	s := sink.NewMemorySink()
	e, err := engine.New(cfg, engine.Options{Sink: s, Store: checkpoint.NewMemoryStore()})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(0, 0)
	// 事件 t=7s 属于滑动窗口 [0,10) 与 [5,15)。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "e1", Key: "k", Timestamp: base.Add(7 * time.Second), Value: 4},
	}); err != nil {
		t.Fatal(err)
	}
	// 水位线推进到 16s：[0,10) 与 [5,15) 均应触发，各自 value=4。
	if err := e.AdvanceWatermark(base.Add(16 * time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	recs := s.Records()
	if len(recs) != 2 {
		t.Fatalf("want 2 sliding windows fired, got %d: %+v", len(recs), recs)
	}
	for _, r := range recs {
		if r.Value != 4 || r.WindowKind != "sliding" || r.IsCorrection {
			t.Fatalf("unexpected record: %+v", r)
		}
	}
	if recs[0].WindowID >= recs[1].WindowID {
		t.Fatalf("window outputs not ordered by window: %s then %s", recs[0].WindowID, recs[1].WindowID)
	}
}

func TestSessionWindowGapAndMerge(t *testing.T) {
	cfg := engine.DefaultConfig()
	cfg.WindowType = engine.WindowSession
	cfg.SessionGap = 5 * time.Second
	cfg.WatermarkLag = 0
	cfg.AllowedLateness = 0
	s := sink.NewMemorySink()
	e, err := engine.New(cfg, engine.Options{Sink: s, Store: checkpoint.NewMemoryStore()})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(100, 0)
	// t=1,4 -> 同一会话（gap 3 < 5）；t=20 -> 独立会话。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "s1", Key: "u", Timestamp: base.Add(1 * time.Second), Value: 1},
		{ID: "s2", Key: "u", Timestamp: base.Add(4 * time.Second), Value: 2},
		{ID: "s3", Key: "u", Timestamp: base.Add(20 * time.Second), Value: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.AdvanceWatermark(base.Add(40 * time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	recs := s.Records()
	if len(recs) != 2 {
		t.Fatalf("want 2 sessions, got %d: %+v", len(recs), recs)
	}
	// 按 end 排序：第一会话 end=4+5=9 value=3；第二会话 end=25 value=3。
	if recs[0].Value != 3 || recs[1].Value != 3 {
		t.Fatalf("unexpected session values: %+v %+v", recs[0], recs[1])
	}
}

// 迟到事件把两个已分别触发的会话重新连接：必须出现一条撤销 + 一条修正。
func TestSessionLateMergeRetractAndRevise(t *testing.T) {
	cfg := engine.DefaultConfig()
	cfg.WindowType = engine.WindowSession
	cfg.SessionGap = 5 * time.Second
	cfg.WatermarkLag = 0
	cfg.AllowedLateness = 60 * time.Second
	s := sink.NewMemorySink()
	e, err := engine.New(cfg, engine.Options{Sink: s, Store: checkpoint.NewMemoryStore()})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(100, 0)
	// 第一批：t=1 会话 [1,6)；推进水位线到 12s 触发它（value=1）。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "s1", Key: "u", Timestamp: base.Add(1 * time.Second), Value: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.AdvanceWatermark(base.Add(12 * time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	// 第二批：t=8 会话 [8,13)，水位线到 20s，两个会话都已独立触发。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "s2", Key: "u", Timestamp: base.Add(8 * time.Second), Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.AdvanceWatermark(base.Add(20 * time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	before := s.Records()
	if len(before) != 2 {
		t.Fatalf("precondition: want 2 fired sessions, got %+v", before)
	}
	// 迟到事件 t=6：新会话 [6,11) 桥接 [1,6) 与 [8,13)（t=6 与 [1,6) 左闭右开
	// 不重叠？6 不 < end 6 => 不重叠；但与 [8,13) 不重叠（11>8 且 6<13 => 重叠）。
	// 所以这里用 t=5：候选 [5,10) 同时重叠 [1,6) 和 [8,13)。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "late-bridge", Key: "u", Timestamp: base.Add(5 * time.Second), Value: 10},
	}); err != nil {
		t.Fatal(err)
	}
	after := s.Records()
	var retract, revise int
	var mergedValue int64
	for _, r := range after[len(before):] {
		if r.Reason == "session-retract" && r.IsCorrection && r.Value == 0 {
			retract++
		}
		if r.Reason == "session-revise" && r.IsCorrection {
			revise++
			mergedValue = r.Value
		}
	}
	if retract != 1 || revise != 1 {
		t.Fatalf("want 1 retract + 1 revise, got retract=%d revise=%d (all=%+v)", retract, revise, after)
	}
	if mergedValue != 13 {
		t.Fatalf("merged session value=%d want 13", mergedValue)
	}
}

func TestSessionCountMergeNoDoubleCount(t *testing.T) {
	cfg := engine.DefaultConfig()
	cfg.WindowType = engine.WindowSession
	cfg.SessionGap = 5 * time.Second
	cfg.Aggregate = "count"
	cfg.WatermarkLag = 0
	cfg.AllowedLateness = 60 * time.Second
	s := sink.NewMemorySink()
	e, err := engine.New(cfg, engine.Options{Sink: s, Store: checkpoint.NewMemoryStore()})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(100, 0)
	// 先触发会话 [1,6)（1 个事件）。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "s1", Key: "u", Timestamp: base.Add(1 * time.Second), Value: 100},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.AdvanceWatermark(base.Add(12 * time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	// 第二个会话 [8,13)（1 个事件）并触发。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "s2", Key: "u", Timestamp: base.Add(8 * time.Second), Value: 200},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.AdvanceWatermark(base.Add(20 * time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	// 迟到桥接事件 t=5：合并两个已触发会话 -> 修正值应为 3 条（而非 300 或 5）。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "bridge", Key: "u", Timestamp: base.Add(5 * time.Second), Value: 999},
	}); err != nil {
		t.Fatal(err)
	}
	var revised int64 = -1
	for _, r := range s.Records() {
		if r.IsCorrection && r.Reason == "session-revise" {
			revised = r.Value
		}
	}
	if revised != 3 {
		t.Fatalf("count aggregation across merged sessions = %d, want 3", revised)
	}
}
