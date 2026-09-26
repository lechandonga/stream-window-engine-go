package aggregate

import (
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/event"
)

func TestAccumulatorDedup(t *testing.T) {
	a := NewAccumulator()
	mk := func(id string, v int64) event.Event {
		return event.Event{ID: id, Key: "k", Ts: time.Unix(0, 1), Value: v}
	}
	if !a.Add(mk("e1", 5)) {
		t.Fatal("first add should count")
	}
	if a.Add(mk("e1", 5)) {
		t.Fatal("duplicate id must not count again")
	}
	if !a.Add(mk("e2", 3)) {
		t.Fatal("new id should count")
	}
	if a.Value != 8 || a.Count != 2 {
		t.Fatalf("got value=%d count=%d, want 8/2", a.Value, a.Count)
	}
	ids := a.SeenIDs()
	if len(ids) != 2 {
		t.Fatalf("seen ids = %v", ids)
	}
}

// 合并按事件 ID 去重：同 ID 只计一次，值不重复累加。
func TestAccumulatorMergeIntoDedup(t *testing.T) {
	mk := func(id string, v int64) event.Event {
		return event.Event{ID: id, Key: "k", Ts: time.Unix(0, 1), Value: v}
	}
	a := NewAccumulator()
	a.Add(mk("e1", 5))
	a.Add(mk("e2", 7))
	b := NewAccumulator()
	b.Add(mk("e2", 7)) // 与 a 重复（迟到重投路径）
	b.Add(mk("e3", 11))

	if added := a.MergeInto(b); added != 1 {
		t.Fatalf("MergeInto added=%d want 1", added)
	}
	if a.Value != 23 || a.Count != 3 {
		t.Fatalf("merged value=%d count=%d want 23/3", a.Value, a.Count)
	}
	// 再次合并同一来源是幂等的。
	if added := a.MergeInto(b); added != 0 {
		t.Fatalf("re-merge must be idempotent, added=%d", added)
	}
	if a.Value != 23 || a.Count != 3 {
		t.Fatalf("re-merge changed state: %d/%d", a.Value, a.Count)
	}
}

// 检查点往返：SeenIDs/SeenValues 同序恢复后 Value 语义保持。
func TestAccumulatorSeenRoundTrip(t *testing.T) {
	a := NewAccumulator()
	a.Add(event.Event{ID: "x", Key: "k", Ts: time.Unix(0, 1), Value: 4})
	a.Add(event.Event{ID: "y", Key: "k", Ts: time.Unix(0, 2), Value: 6})
	ids, vals := a.SeenIDs(), a.SeenValues()
	if len(ids) != 2 || len(vals) != 2 {
		t.Fatalf("seen export wrong: %v %v", ids, vals)
	}
	// 恢复后按 ID 查值必须一致。
	b := NewAccumulator()
	b.LoadSeen(ids, vals)
	for i, id := range ids {
		if got := b.Seen[id]; got != a.Seen[id] {
			t.Fatalf("value for %s: got %d want %d", id, got, a.Seen[id])
		}
		_ = vals[i]
	}
	// 恢复后的去重仍然生效。
	if b.Add(event.Event{ID: "x", Key: "k", Ts: time.Unix(0, 3), Value: 100}) {
		t.Fatal("restored seen set must dedupe")
	}
}
