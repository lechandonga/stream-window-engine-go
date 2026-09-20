package source

import (
	"errors"
	"io"
	"testing"
	"time"
)

func TestSliceSourceInterruptionAndEOF(t *testing.T) {
	src := NewSliceSource([]EventBatch{
		{Progress: time.Unix(1, 0)},
		{Progress: time.Unix(2, 0)},
	})
	b1, err := src.Poll()
	if err != nil || b1.Progress.Unix() != 1 {
		t.Fatalf("poll1: %v %+v", err, b1)
	}
	src.MarkInterrupted()
	if _, err := src.Poll(); !errors.Is(err, ErrSourceInterrupted) {
		t.Fatalf("want interrupted, got %v", err)
	}

	src2 := NewSliceSource(nil)
	if _, err := src2.Poll(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
}
