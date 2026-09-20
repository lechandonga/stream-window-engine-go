package watermark

import (
	"testing"
	"time"
)

func TestMonotonicNeverRetreats(t *testing.T) {
	g := NewGenerator(2 * time.Second)
	g.Observe(time.Unix(10, 0)) // wm = 8s
	if g.Current().Unix() != 8 {
		t.Fatalf("wm=%d want 8", g.Current().Unix())
	}
	g.Observe(time.Unix(5, 0)) // 迟到事件，不回退
	if g.Current().Unix() != 8 {
		t.Fatalf("wm regressed to %d", g.Current().Unix())
	}
	g.Observe(time.Unix(20, 0))
	if g.Current().Unix() != 18 {
		t.Fatalf("wm=%d want 18", g.Current().Unix())
	}
	g.Advance(time.Unix(15, 0)) // 更小的进度也不回退
	if g.Current().Unix() != 18 {
		t.Fatalf("advance regressed to %d", g.Current().Unix())
	}
	g.Advance(time.Unix(30, 0))
	if g.Current().Unix() != 28 {
		t.Fatalf("wm=%d want 28", g.Current().Unix())
	}
}

func TestRestoreDoesNotRetreat(t *testing.T) {
	g := NewGenerator(time.Second)
	g.Restore(time.Unix(50, 0), time.Time{})
	if g.Current().Unix() != 50 {
		t.Fatalf("restored wm=%d", g.Current().Unix())
	}
	g.Observe(time.Unix(40, 0)) // 恢复后早于水位线的观察也不得回退
	if g.Current().Unix() != 50 {
		t.Fatalf("wm regressed after restore: %d", g.Current().Unix())
	}
}
