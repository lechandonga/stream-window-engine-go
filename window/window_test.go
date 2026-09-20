package window

import (
	"testing"
	"time"
)

func ms(n int64) time.Time { return time.UnixMilli(n) }

func TestTumblingAssignmentBoundaries(t *testing.T) {
	a := NewTumblingAssigner(10 * time.Second)
	cases := []struct {
		ts time.Time
		id string
	}{
		{ms(0), "tumbling[0,10000)"},
		{ms(9999), "tumbling[0,10000)"},
		{ms(10000), "tumbling[10000,20000)"},
		{ms(-1), "tumbling[-10000,0)"},
	}
	for _, c := range cases {
		got := a.Assign(c.ts)
		if len(got) != 1 || got[0].ID(Tumbling) != c.id {
			t.Fatalf("ts=%d got=%+v want id=%s", c.ts.UnixMilli(), got, c.id)
		}
	}
	w := a.Assign(ms(0))[0]
	if w.Contains(ms(10000)) || !w.Contains(ms(0)) || !w.Contains(ms(9999)) {
		t.Fatalf("half-open containment wrong: %+v", w)
	}
}

func TestSlidingMultiAssignmentOrdered(t *testing.T) {
	a := NewSlidingAssigner(10*time.Second, 5*time.Second)
	got := a.Assign(ms(7000))
	want := []string{"sliding[0,10000)", "sliding[5000,15000)"}
	if len(got) != 2 {
		t.Fatalf("want 2 windows, got %d", len(got))
	}
	for i, w := range got {
		if w.ID(Sliding) != want[i] {
			t.Fatalf("window %d = %s want %s", i, w.ID(Sliding), want[i])
		}
	}
	// 边界时间 10000 应属于 [5000,15000) 和 [10000,20000)。
	got = a.Assign(ms(10000))
	if got[0].Start.UnixMilli() != 5000 || got[1].Start.UnixMilli() != 10000 {
		t.Fatalf("boundary assignment wrong: %+v", got)
	}
}

func TestSessionInitialWindow(t *testing.T) {
	a := NewSessionAssigner(5 * time.Second)
	got := a.Assign(ms(1000))
	if len(got) != 1 || got[0].Start != ms(1000) || got[0].End != ms(6000) {
		t.Fatalf("session initial window wrong: %+v", got)
	}
	if a.Gap() != 5*time.Second {
		t.Fatalf("gap=%v", a.Gap())
	}
}
