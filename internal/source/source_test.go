package source

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/event"
)

func mkEvents(n int) []event.Event {
	evs := make([]event.Event, n)
	for i := 0; i < n; i++ {
		evs[i] = event.Event{ID: string(rune('a' + i)), Key: "k", Ts: time.Unix(int64(i), 0), Value: int64(i)}
	}
	return evs
}

func drain(ch <-chan Record) []Record {
	var out []Record
	for r := range ch {
		out = append(out, r)
	}
	return out
}

func TestInterruptIsDetectable(t *testing.T) {
	src := NewMemoryWithFailure(mkEvents(10), 4)
	ch, err := src.Subscribe(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	got := drain(ch)
	if len(got) != 4 {
		t.Fatalf("delivered=%d want 4", len(got))
	}
	if !errors.Is(src.Err(), ErrInterrupted) {
		t.Fatalf("want ErrInterrupted, got %v", src.Err())
	}
}

func TestReplayFromOffsetIdentical(t *testing.T) {
	evs := mkEvents(8)
	src := NewMemory(evs)
	first := drain(mustSub(t, src, 0))
	// 中断后从偏移 3 重放，必须得到与首次订阅 [3:] 完全相同的序列。
	replayed := drain(mustSub(t, src, 3))
	if len(replayed) != 5 {
		t.Fatalf("replay len=%d want 5", len(replayed))
	}
	for i, r := range replayed {
		if r.Offset != first[i+3].Offset || r.Event.ID != first[i+3].Event.ID {
			t.Fatalf("replay mismatch at %d", i)
		}
	}
	if src.Err() != nil {
		t.Fatalf("clean replay should have nil err, got %v", src.Err())
	}
}

func TestResumeAtEndOffset(t *testing.T) {
	src := NewMemory(mkEvents(3))
	ch, _ := src.Subscribe(context.Background(), 3) // 已耗尽
	if n := len(drain(ch)); n != 0 {
		t.Fatalf("want 0 records at end offset, got %d", n)
	}
	if src.Err() != nil {
		t.Fatalf("want nil err at end offset, got %v", src.Err())
	}
}

func TestInterruptedResumeDoesNotDuplicate(t *testing.T) {
	evs := mkEvents(6)
	src1 := NewMemoryWithFailure(evs, 2)
	first := drain(mustSub(t, src1, 0))
	if !errors.Is(src1.Err(), ErrInterrupted) {
		t.Fatal("want interruption")
	}
	// 模拟"已提交前 2 条"，从中断偏移 2 续传。
	src2 := NewMemory(evs)
	second := drain(mustSub(t, src2, int64(len(first))))
	if len(second) != 4 {
		t.Fatalf("resume delivered=%d want 4", len(second))
	}
	seen := map[string]int{}
	for _, r := range append(first, second...) {
		seen[r.Event.ID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("event %s delivered %d times across resume", id, n)
		}
	}
}

func mustSub(t *testing.T, src Source, offset int64) <-chan Record {
	t.Helper()
	ch, err := src.Subscribe(context.Background(), offset)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}
