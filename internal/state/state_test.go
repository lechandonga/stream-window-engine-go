package state

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

func testKey(g string, startS int64) Key {
	w := window.Window{Kind: window.Tumbling, Start: time.Unix(startS, 0), End: time.Unix(startS+10, 0)}
	return Key{Group: g, Win: w}
}

func ev(id string, tsSec int64, v int64) event.Event {
	return event.Event{ID: id, Key: "k", Ts: time.Unix(tsSec, 0), Value: v}
}

func TestApplyDedupAndGet(t *testing.T) {
	s, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	k := testKey("k", 0)
	added, err := s.Apply(ctx, k, ev("e1", 1, 5))
	if err != nil || !added {
		t.Fatalf("first apply: added=%v err=%v", added, err)
	}
	added, err = s.Apply(ctx, k, ev("e1", 1, 5))
	if err != nil || added {
		t.Fatalf("duplicate apply: added=%v err=%v", added, err)
	}
	added, _ = s.Apply(ctx, k, ev("e2", 2, 3))
	if !added {
		t.Fatal("second event should be added")
	}
	en, ok, _ := s.Get(ctx, k)
	if !ok || en.Acc.Value != 8 || en.Acc.Count != 2 {
		t.Fatalf("state wrong: %+v", en)
	}
}

func TestDeterministicListKeys(t *testing.T) {
	s, _ := New(Config{})
	ctx := context.Background()
	// 乱序写入。
	for _, sk := range []int64{30, 0, 20, 10} {
		_, _ = s.Apply(ctx, testKey("b", sk), ev("x", sk, 1))
	}
	for _, sk := range []int64{10, 0} {
		_, _ = s.Apply(ctx, testKey("a", sk), ev("x", sk, 1))
	}
	keys, err := s.ListKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := [][2]interface{}{}
	for _, k := range keys {
		got = append(got, [2]interface{}{k.Group, k.Win.Start.Unix()})
	}
	want := [][2]interface{}{
		{"a", int64(0)}, {"a", int64(10)},
		{"b", int64(0)}, {"b", int64(10)}, {"b", int64(20)}, {"b", int64(30)},
	}
	if len(got) != len(want) {
		t.Fatalf("keys=%v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("index %d got %v want %v (full=%v)", i, got[i], want[i], got)
		}
	}
}

func TestSpillEvictsAndReloads(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{MaxActiveKeys: 2, Overflow: OverflowSpill, SpillDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	k0, k1, k2 := testKey("k", 0), testKey("k", 10), testKey("k", 20)
	if _, err := s.Apply(ctx, k0, ev("a", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(ctx, k1, ev("b", 11, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(ctx, k2, ev("c", 21, 3)); err != nil {
		t.Fatal(err)
	}
	if s.ActiveCount() != 2 {
		t.Fatalf("active=%d want 2", s.ActiveCount())
	}
	if s.SpillCount() == 0 {
		t.Fatal("expected spill to happen")
	}
	// 读回最旧键 k0（已落盘），值必须正确，且此时内存又淘汰另一键。
	en, ok, err := s.Get(ctx, k0)
	if err != nil || !ok {
		t.Fatalf("reload spilled: ok=%v err=%v", ok, err)
	}
	if en.Acc.Value != 1 || en.Acc.Count != 1 {
		t.Fatalf("reloaded state wrong: value=%d count=%d", en.Acc.Value, en.Acc.Count)
	}
	keys, _ := s.ListKeys(ctx)
	if len(keys) != 3 {
		t.Fatalf("ListKeys must include spilled keys, got %d", len(keys))
	}
}

func TestSpillSnapshotRestoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, _ := New(Config{MaxActiveKeys: 1, Overflow: OverflowSpill, SpillDir: dir})
	ctx := context.Background()
	_, _ = s.Apply(ctx, testKey("k", 0), ev("a", 1, 1))
	_, _ = s.Apply(ctx, testKey("k", 10), ev("b", 11, 2))
	_, _ = s.Apply(ctx, testKey("k", 20), ev("c", 21, 3))

	snaps, err := s.SnapshotEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 3 {
		t.Fatalf("snapshot entries=%d want 3", len(snaps))
	}

	s2, _ := New(Config{MaxActiveKeys: 1, Overflow: OverflowSpill, SpillDir: t.TempDir()})
	if err := s2.RestoreEntries(ctx, snaps); err != nil {
		t.Fatal(err)
	}
	keys, _ := s2.ListKeys(ctx)
	if len(keys) != 3 {
		t.Fatalf("restored keys=%d want 3", len(keys))
	}
	wantVal := map[int64]int64{0: 1, 10: 2, 20: 3}
	for _, k := range keys {
		en, ok, err := s2.Get(ctx, k)
		if err != nil || !ok {
			t.Fatalf("entry %s missing: %v", window.WindowID(k.Win), err)
		}
		if en.Acc.Value != wantVal[k.Win.Start.Unix()] || en.Acc.Count != 1 {
			t.Fatalf("entry %d restored wrong: %+v", k.Win.Start.Unix(), en.Acc)
		}
	}
}

func TestPurgeOnlyClosedEmitted(t *testing.T) {
	s, _ := New(Config{})
	ctx := context.Background()
	k0, k1 := testKey("k", 0), testKey("k", 10)
	_, _ = s.Apply(ctx, k0, ev("a", 1, 1))
	_, _ = s.Apply(ctx, k1, ev("b", 11, 1))
	// k0 已发出且关闭；k1 仅关闭未发出。
	_ = s.MarkEmitted(k0, 1)
	_ = s.Close(k0)
	_ = s.Close(k1)
	n, err := s.Purge(time.Unix(15, 0).UnixNano())
	if err != nil || n != 1 {
		t.Fatalf("purge n=%d err=%v, want 1", n, err)
	}
	keys, _ := s.ListKeys(ctx)
	if len(keys) != 1 || keys[0] != k1 {
		t.Fatalf("unexpected remaining keys: %+v", keys)
	}
}

func TestBlockPolicyBackpressure(t *testing.T) {
	s, _ := New(Config{MaxActiveKeys: 1, Overflow: OverflowBlock})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _ = s.Apply(ctx, testKey("k", 0), ev("a", 1, 1))

	done := make(chan error, 1)
	go func() {
		_, err := s.Apply(ctx, testKey("k", 10), ev("b", 11, 1))
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("Apply should block under memory cap")
	case <-time.After(30 * time.Millisecond):
	}
	// 清理释放空间后写入解除阻塞。
	_ = s.MarkEmitted(testKey("k", 0), 1)
	_ = s.Close(testKey("k", 0))
	if _, err := s.Purge(time.Unix(15, 0).UnixNano()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked apply returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Apply did not unblock after purge")
	}
}
