package engine_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/engine"
	"github.com/lechandonga/stream-window-engine-go/event"
	"github.com/lechandonga/stream-window-engine-go/sink"
)

func buildEvents() []event.Event {
	base := time.Unix(1000, 0)
	return []event.Event{
		{ID: "1", Key: "A", Timestamp: base.Add(1 * time.Second), Value: 1},
		{ID: "2", Key: "B", Timestamp: base.Add(2 * time.Second), Value: 2},
		{ID: "3", Key: "A", Timestamp: base.Add(11 * time.Second), Value: 10},
		{ID: "4", Key: "B", Timestamp: base.Add(1 * time.Second), Value: 20},
		{ID: "5", Key: "A", Timestamp: base.Add(12 * time.Second), Value: 100},
		{ID: "6", Key: "C", Timestamp: base.Add(3 * time.Second), Value: 3},
	}
}

func runAndCollect(t *testing.T, shards int) []sink.Record {
	t.Helper()
	cfg := baseCfg()
	cfg.ShardCount = shards
	cfg.AllowedLateness = 60 * time.Second
	s := sink.NewMemorySink()
	e, err := engine.New(cfg, engine.Options{Sink: s, Store: checkpoint.NewMemoryStore()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.ProcessBatch(buildEvents()); err != nil {
		t.Fatal(err)
	}
	if err := e.Shutdown(); err != nil {
		t.Fatal(err)
	}
	return s.Records()
}

func sig(recs []sink.Record) string {
	out := ""
	for _, r := range recs {
		out += fmt.Sprintf("%s|%s|%s|%d|%v;", r.Key, r.WindowID, r.Reason, r.Value, r.IsCorrection)
	}
	return out
}

func TestConcurrencyEquivalentToSerial(t *testing.T) {
	serial := runAndCollect(t, 1)
	for _, n := range []int{2, 3, 4, 8} {
		got := runAndCollect(t, n)
		if sig(got) != sig(serial) {
			t.Fatalf("shards=%d output differs:\n serial=%s\n got   =%s", n, sig(serial), sig(got))
		}
	}
}

func TestReplaySameOutputSequence(t *testing.T) {
	first := runAndCollect(t, 4)
	second := runAndCollect(t, 4)
	if sig(first) != sig(second) {
		t.Fatalf("replay differs:\n first=%s\n second=%s", sig(first), sig(second))
	}
	var hasBFire bool
	for _, r := range first {
		if r.Key == "B" && !r.IsCorrection && r.Reason == "window-fire" && r.Value == 22 {
			hasBFire = true
		}
	}
	if !hasBFire {
		t.Fatalf("B should fire once with value 22 (same-batch late event joins first fire): %s", sig(first))
	}
}

func TestOutOfOrderAcrossBatchesProducesCorrection(t *testing.T) {
	cfg := baseCfg()
	cfg.AllowedLateness = 60 * time.Second
	s := sink.NewMemorySink()
	e, err := engine.New(cfg, engine.Options{Sink: s, Store: checkpoint.NewMemoryStore()})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1000, 0)
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "1", Key: "B", Timestamp: base.Add(2 * time.Second), Value: 2},
		{ID: "2", Key: "B", Timestamp: base.Add(11 * time.Second), Value: 0},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "3", Key: "B", Timestamp: base.Add(1 * time.Second), Value: 20},
	}); err != nil {
		t.Fatal(err)
	}
	recs := s.Records()
	var corr *sink.Record
	for i := range recs {
		if recs[i].IsCorrection {
			corr = &recs[i]
		}
	}
	if corr == nil || corr.Value != 22 {
		t.Fatalf("want correction value 22, got %+v", recs)
	}
}

// 大批量、多 key、固定置换乱序：多分片并行应用的输出必须与单分片串行一致，
// 重复多次结果稳定。
func TestParallelApplicationMatchesSerial(t *testing.T) {
	base := time.Unix(20000, 0)
	keys := []string{"A", "B", "C", "D", "E", "F", "G", "H"}
	var evs []event.Event
	id := 0
	for _, k := range keys {
		for i := int64(0); i < 50; i++ {
			id++
			win := i % 5
			win2 := int64(win * 10)
			off := time.Duration(win2+i%9) * time.Second
			evs = append(evs, event.Event{
				ID: fmt.Sprintf("e-%s-%d", k, id), Key: k,
				Timestamp: base.Add(off), Value: i,
			})
		}
	}
	shuffled := make([]event.Event, len(evs))
	copy(shuffled, evs)
	for i := len(shuffled) - 1; i > 0; i-- {
		j := (i*31 + 7) % (i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	shuffled = append(shuffled, event.Event{ID: "trigger", Key: "A", Timestamp: base.Add(60 * time.Second), Value: 0})

	run := func(shards int) string {
		cfg := baseCfg()
		cfg.ShardCount = shards
		cfg.AllowedLateness = 3600 * time.Second
		s := sink.NewMemorySink()
		e, err := engine.New(cfg, engine.Options{Sink: s, Store: checkpoint.NewMemoryStore()})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < len(shuffled); i += 13 {
			end := i + 13
			if end > len(shuffled) {
				end = len(shuffled)
			}
			if _, err := e.ProcessBatch(shuffled[i:end]); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.Shutdown(); err != nil {
			t.Fatal(err)
		}
		return sig(s.Records())
	}
	want := run(1)
	for iter := 0; iter < 3; iter++ {
		for _, n := range []int{2, 4, 16} {
			if got := run(n); got != want {
				t.Fatalf("iter=%d shards=%d mismatch:\nserial=%s\ngot=%s", iter, n, want, got)
			}
		}
	}
}
