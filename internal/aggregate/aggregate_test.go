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
