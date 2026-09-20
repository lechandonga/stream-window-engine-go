package sink

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
)

func out(seq, rev int64, kind aggregate.OutputKind) aggregate.Output {
	return aggregate.Output{Seq: seq, Revision: rev, Kind: kind, Key: "k", Value: seq}
}

func TestMemorySinkIdempotent(t *testing.T) {
	m := NewMemory()
	for i := 0; i < 3; i++ { // 重复投递 3 次
		_ = m.Deliver(out(1, 1, aggregate.Insert))
		_ = m.Deliver(out(2, 1, aggregate.Insert))
		_ = m.Deliver(out(3, 2, aggregate.Correction))
	}
	if got := len(m.Outputs()); got != 3 {
		t.Fatalf("outputs=%d want 3 (redelivery must not duplicate)", got)
	}
	if m.LastSeq() != 3 {
		t.Fatalf("lastseq=%d want 3", m.LastSeq())
	}
	_ = m.DeliverSide(SideRecord{Decision: event.Decision{EventID: "x"}})
	if len(m.Sides()) != 1 {
		t.Fatal("side record missing")
	}
}

func TestFileSinkRecoverAndDedup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.jsonl")
	write := func() {
		f, err := NewFile(path)
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Deliver(out(1, 1, aggregate.Insert))
		_ = f.Deliver(out(2, 2, aggregate.Correction))
		// 模拟重投相同 Seq。
		_ = f.Deliver(out(1, 1, aggregate.Insert))
		f.Close()
	}
	write()
	// 重新打开（恢复）：LastSeq 与已确认输出必须还在。
	f, err := NewFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.LastSeq() != 2 {
		t.Fatalf("recovered lastseq=%d want 2", f.LastSeq())
	}
	if len(f.Outputs()) != 2 {
		t.Fatalf("recovered outputs=%d want 2", len(f.Outputs()))
	}
	// 恢复后再次重投 Seq=1 不得追加。
	_ = f.Deliver(out(1, 1, aggregate.Insert))
	_ = f.Deliver(out(3, 1, aggregate.Insert))
	f.Close()

	b, _ := os.ReadFile(path)
	if n := countNewlines(b); n != 3 {
		t.Fatalf("file lines=%d want 3", n)
	}
}

func countNewlines(b []byte) int {
	n := 0
	for _, c := range b {
		if c == '\n' {
			n++
		}
	}
	return n
}
