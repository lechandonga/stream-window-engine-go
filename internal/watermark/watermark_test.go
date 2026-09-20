package watermark

import (
	"testing"
	"time"
)

func TestMonotonicNeverRegress(t *testing.T) {
	w := New()
	if w.IsSet() {
		t.Fatal("new watermark should be unset")
	}
	if !w.Advance(time.Unix(0, 100)) {
		t.Fatal("advance to 100 should report true")
	}
	if w.Advance(time.Unix(0, 50)) {
		t.Fatal("advance backwards must not happen")
	}
	if got := w.Get().UnixNano(); got != 100 {
		t.Fatalf("watermark regressed: %d", got)
	}
	if w.Advance(time.Time{}) {
		t.Fatal("zero time must not change watermark")
	}
	// 恢复后重放旧事件也不回退。
	w2 := FromTime(time.Unix(0, 1000))
	g := NewGenerator(10 * time.Second)
	proposed := g.Observe(w2.Get(), time.Unix(0, 500))
	w2.Advance(proposed)
	if w2.Get().UnixNano() != 1000 {
		t.Fatalf("watermark changed after replay of older event: %d", w2.Get().UnixNano())
	}
}

func TestGeneratorLag(t *testing.T) {
	g := NewGenerator(10)
	proposed := g.Observe(time.Time{}, time.Unix(0, 100))
	if proposed.UnixNano() != 90 {
		t.Fatalf("expected 100-10=90, got %d", proposed.UnixNano())
	}
	// 更旧的观测不降低建议值。
	proposed = g.Observe(proposed, time.Unix(0, 50))
	if proposed.UnixNano() != 90 {
		t.Fatalf("proposal dropped: %d", proposed.UnixNano())
	}
	proposed = g.Observe(proposed, time.Unix(0, 200))
	if proposed.UnixNano() != 190 {
		t.Fatalf("expected 190, got %d", proposed.UnixNano())
	}
}
