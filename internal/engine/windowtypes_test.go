package engine

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/source"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// 滑动窗口：一个事件计入多个窗口；乱序到达结果确定。
func TestSlidingWindow(t *testing.T) {
	cfg := Config{
		Window:      window.Spec{Kind: window.Sliding, Size: 10 * time.Second, Slide: 5 * time.Second},
		Lag:         30 * time.Second,
		Late:        event.LateDrop,
		FutureBound: 24 * time.Hour,
		State:       state.Config{},
	}
	e := quietEngine(t, cfg)
	events := []event.Event{
		eid("a", "k", 2, 10),
		eid("b", "k", 7, 20),
		eid("c", "k", 12, 30),
	}
	dst := sink.NewMemory()
	if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
		t.Fatal(err)
	}
	out := dst.Outputs()
	// 覆盖窗口（size10/slide5）：
	// [0,10): a(10)+b(20)=30；[5,15): b(20)+c(30)=50；[10,20): c(30)。
	got := map[int64][2]int64{} // start -> {value,count}
	for _, o := range out {
		rel := o.Window.Start.Sub(testBase).Nanoseconds()
		got[rel] = [2]int64{o.Value, o.Count}
	}
	want := map[int64][2]int64{
		-5e9: {10, 1}, // [-5,5) 只含 a
		0:    {30, 2},
		5e9:  {50, 2},
		10e9: {30, 1},
	}
	if len(got) != len(want) {
		t.Fatalf("windows=%v want %v", got, want)
	}
	for start, wv := range want {
		if got[start] != wv {
			t.Fatalf("window start=%d got %v want %v", start, got[start], wv)
		}
	}
	// 输出顺序必须按窗口起点确定。
	for i := 1; i < len(out); i++ {
		if out[i].Window.Start.Before(out[i-1].Window.Start) {
			t.Fatalf("output order not deterministic by start: %+v", out)
		}
	}
}

// 会话窗口：间隔 <= gap 的事件合并；到达顺序不影响最终会话。
func TestSessionWindowOrderIndependent(t *testing.T) {
	// lag 覆盖输入乱序跨度（1s..20s），会话在 flush 前不提前触发，
	// 从而保证不同到达顺序产生相同的合并结果。
	cfg := Config{
		Window:      window.Spec{Kind: window.Session, Gap: 3 * time.Second},
		Lag:         25 * time.Second,
		Late:        event.LateDrop,
		FutureBound: 24 * time.Hour,
		State:       state.Config{},
	}
	// 两组会话：[0,5) 内 1,3,5；孤立事件 20。
	eventsOrderA := []event.Event{
		eid("e1", "k", 1, 1), eid("e2", "k", 5, 2), eid("e3", "k", 3, 4), eid("e4", "k", 20, 8),
	}
	eventsOrderB := []event.Event{
		eventsOrderA[3], eventsOrderA[1], eventsOrderA[2], eventsOrderA[0],
	}
	run := func(evs []event.Event) []aggregate.Output {
		e := quietEngine(t, cfg)
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(evs), dst); err != nil {
			t.Fatal(err)
		}
		return dst.Outputs()
	}
	a := run(eventsOrderA)
	b := run(eventsOrderB)
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("session outputs a=%d b=%d", len(a), len(b))
	}
	for i := range a {
		oa, ob := a[i], b[i]
		if !oa.Window.Start.Equal(ob.Window.Start) || !oa.Window.End.Equal(ob.Window.End) ||
			oa.Value != ob.Value || oa.Count != ob.Count {
			t.Fatalf("session order-dependence:\n a=%+v\n b=%+v", oa, ob)
		}
	}
	// 第一个会话 [1, 8)（事件 1,3,5，末事件 5+gap3=8），值 1+2+4=7。
	sess := a[0]
	if !sess.Window.Start.Equal(testBase.Add(1*time.Second)) ||
		!sess.Window.End.Equal(testBase.Add(8*time.Second)) ||
		sess.Value != 7 || sess.Count != 3 {
		t.Fatalf("merged session wrong: start=%v end=%v value=%d count=%d",
			sess.Window.Start, sess.Window.End, sess.Value, sess.Count)
	}
}

// 多分组并发：结果与串行一致（乱序打乱多 key 事件）。
func TestMultiGroupConcurrentEquivalence(t *testing.T) {
	mkEvents := func() []event.Event {
		var evs []event.Event
		for i := int64(0); i < 40; i++ {
			for _, g := range []string{"g1", "g2", "g3", "g4"} {
				evs = append(evs, eid("e-"+g+"-"+itoa(i), g, i, i))
			}
		}
		// 确定性乱序（步长互质洗牌）。
		shuffled := make([]event.Event, len(evs))
		for i := range evs {
			shuffled[(i*37)%len(evs)] = evs[i]
		}
		return shuffled
	}
	cfg := func(conc int) Config {
		c := tumblingCfg(60*time.Second, event.LateDrop, 0)
		c.Concurrency = conc
		return c
	}
	evs := mkEvents()
	run := func(conc int) []aggregate.Output {
		e := quietEngine(t, cfg(conc))
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(evs), dst); err != nil {
			t.Fatal(err)
		}
		return dst.Outputs()
	}
	serial := run(1)
	concurrent := run(8)
	if len(serial) != len(concurrent) {
		t.Fatalf("len serial=%d concurrent=%d", len(serial), len(concurrent))
	}
	for i := range serial {
		a, b := serial[i], concurrent[i]
		if a.Seq != b.Seq || a.Key != b.Key || a.Value != b.Value || a.Count != b.Count ||
			!a.Window.Start.Equal(b.Window.Start) {
			t.Fatalf("concurrent divergence at %d:\n serial=%+v\n concurrent=%+v", i, a, b)
		}
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
