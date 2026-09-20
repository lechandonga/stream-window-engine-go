package engine_test

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/engine"
	"github.com/lechandonga/stream-window-engine-go/sink"
)

func mkEngineWith(t *testing.T, cfg engine.Config, s *sink.MemorySink, st checkpoint.Store) (*engine.Engine, *sink.MemorySink) {
	t.Helper()
	e, err := engine.New(cfg, engine.Options{Sink: s, Store: st})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return e, s
}

func countSinkLines(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, b := range data {
		if b == '\n' {
			n++
		}
	}
	return n
}

func appendByteToFile(path string, b byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write([]byte{b}); err != nil {
		return err
	}
	return f.Sync()
}

func errorsIs(err, target error) bool { return errors.Is(err, target) }
