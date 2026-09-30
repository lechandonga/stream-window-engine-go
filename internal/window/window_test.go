package window

import (
	"testing"
	"time"
)

func tns(ns int64) time.Time { return time.Unix(0, ns) }

func TestTumblingBoundaries(t *testing.T) {
	a, err := NewAssigner(Spec{Kind: Tumbling, Size: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	// 边界事件归后一个窗口（半开 [Start,End)）。
	ws := a.Assign(tns(10_000_000_000))
	if got := ws[0].Start.UnixNano(); got != 10_000_000_000 {
		t.Fatalf("boundary event should belong to later window, got start=%d", got)
	}
	ws = a.Assign(tns(9_999_999_999))
	if got := ws[0].Start.UnixNano(); got != 0 {
		t.Fatalf("pre-boundary event start=%d", got)
	}
	// 确定性：多次分配一致。
	w1 := a.Assign(tns(1234567))
	w2 := a.Assign(tns(1234567))
	if WindowID(w1[0]) != WindowID(w2[0]) {
		t.Fatal("assignment nondeterministic")
	}
}

func TestSlidingOverlapCount(t *testing.T) {
	a, err := NewAssigner(Spec{Kind: Sliding, Size: 10 * time.Second, Slide: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	// size=10s/slide=5s 时窗口为 ...[-5,5),[0,10),[5,15)...
	// ts=7s 同时属于 [0,10) 与 [5,15) 两个窗口。
	ws := a.Assign(tns(7_000_000_000))
	if len(ws) != 2 {
		t.Fatalf("ts=7s expected 2 windows, got %d: %+v", len(ws), ws)
	}
	if ws[0].Start.UnixNano() != 0 || ws[1].Start.UnixNano() != 5_000_000_000 {
		t.Fatalf("unexpected windows: %+v", ws)
	}
	// ts=1s 只属于 [0,10)（[-5,5) 右端开，1 在其中 -> 也属于？1<5 是属于的）。
	// 实际上 ts=1s 属于 [-5,5) 与 [0,10)；选 ts=-1s 验证更早窗口。
	ws = a.Assign(tns(5_000_000_000))
	if len(ws) != 2 {
		t.Fatalf("ts=5s expected 2 windows, got %d: %+v", len(ws), ws)
	}
	if ws[0].Start.UnixNano() != 0 || ws[1].Start.UnixNano() != 5_000_000_000 {
		t.Fatalf("unexpected windows at boundary: %+v", ws)
	}
	// ts=0 属于 [-5,5) 与 [0,10)，验证负时间与起点边界。
	ws = a.Assign(tns(0))
	if len(ws) != 2 || ws[0].Start.UnixNano() != -5_000_000_000 || ws[1].Start.UnixNano() != 0 {
		t.Fatalf("ts=0 expected windows starting at -5s,0s, got %+v", ws)
	}
}

func tsec(s int64) time.Time { return time.Unix(s, 0) }

func TestMergeSessionsOrderIndependent(t *testing.T) {
	gap := 2 * time.Second
	mk := func(s, e, seq int64) Window {
		return Window{Kind: Session, Start: tsec(s), End: tsec(e), Seq: seq}
	}
	a := []Window{mk(0, 3, 2), mk(10, 13, 4), mk(3, 6, 1), mk(20, 22, 9)}
	b := []Window{a[3], a[1], a[2], a[0]}
	ma := MergeSessions(a, gap)
	mb := MergeSessions(b, gap)
	if len(ma) != len(mb) {
		t.Fatalf("cluster count differs: %d vs %d", len(ma), len(mb))
	}
	for i := range ma {
		if WindowID(ma[i]) != WindowID(mb[i]) {
			t.Fatalf("cluster %d differs: %+v vs %+v", i, ma[i], mb[i])
		}
	}
	// [0,3) 与 [3,6) 间隔 0 <= gap -> 合并为 [0,6)，Seq 取最小值 1。
	if ma[0].Start.Unix() != 0 || ma[0].End.Unix() != 6 || ma[0].Seq != 1 {
		t.Fatalf("unexpected merged cluster: %+v", ma[0])
	}
	// [0,6) 与 [10,13) 间隔 4s > 2s，独立；[20,22) 同样独立。
	if len(ma) != 3 {
		t.Fatalf("expected 3 clusters, got %d: %+v", len(ma), ma)
	}
	// 初始会话为 [ts, ts+gap)；两个首尾相接的单事件会话
	// [10,12) 与 [12,14) 表示相邻事件时间差恰好等于 gap=2s，应当合并。
	touch := MergeSessions([]Window{mk(10, 12, 1), mk(12, 14, 2)}, gap)
	if len(touch) != 1 || touch[0].Start.Unix() != 10 || touch[0].End.Unix() != 14 {
		t.Fatalf("gap-touching sessions should merge, got %+v", touch)
	}
	// 超过 gap 1ns 也必须独立：[10,12) 与 [12.000000001, 14.000000001)。
	t1 := time.Unix(10, 0)
	over := MergeSessions([]Window{
		{Kind: Session, Start: t1, End: t1.Add(gap), Seq: 1},
		{Kind: Session, Start: t1.Add(gap + time.Nanosecond), End: t1.Add(2*gap + time.Nanosecond), Seq: 2},
	}, gap)
	if len(over) != 2 {
		t.Fatalf("gap exceeded by 1ns must split sessions, got %+v", over)
	}
}

func TestInvalidSpec(t *testing.T) {
	if _, err := NewAssigner(Spec{Kind: Tumbling, Size: 0}); err == nil {
		t.Fatal("expected error for zero tumbling size")
	}
	if _, err := NewAssigner(Spec{Kind: Session, Gap: 0}); err == nil {
		t.Fatal("expected error for zero session gap")
	}
}
