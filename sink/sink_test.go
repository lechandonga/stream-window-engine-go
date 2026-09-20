package sink

import (
	"testing"
)

func TestMemorySinkIdempotentBySeq(t *testing.T) {
	s := NewMemorySink()
	r := Record{Seq: 1, Key: "k", Value: 5}
	if err := s.Emit(r); err != nil {
		t.Fatal(err)
	}
	if err := s.Emit(r); err != nil { // 重复投递
		t.Fatal(err)
	}
	r2 := Record{Seq: 2, Key: "k", Value: 6}
	_ = s.Emit(r2)
	if len(s.Records()) != 2 {
		t.Fatalf("records=%d want 2", len(s.Records()))
	}
	if last, ok := s.LastSeq(); !ok || last != 2 {
		t.Fatalf("last=%d,%v", last, ok)
	}
}
