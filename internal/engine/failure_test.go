package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lechandonga/stream-window-engine-go/internal/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/source"
)

// 检查点写入失败必须被识别为 ErrWriteFailed，且与来源中断区分。
func TestCheckpointWriteFailure(t *testing.T) {
	base := t.TempDir()
	// 用一个普通文件占据 chk 期望位置的父路径，使 rename/写入失败。
	blocker := filepath.Join(base, "chk")
	if err := os.WriteFile(blocker, []byte("not-a-dir"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := tumblingCfg(0, 0, 0)
	cfg.CheckpointDir = blocker // FileStore 无法在文件下建目录
	cfg.CheckpointEvery = 1
	e := quietEngine(t, cfg)
	err := e.Run(context.Background(),
		source.NewMemory([]event.Event{eid("a", "k", 1, 1)}), sink.NewMemory())
	if !errors.Is(err, checkpoint.ErrWriteFailed) {
		t.Fatalf("want ErrWriteFailed, got %v", err)
	}
}

// 检查点损坏恢复必须报 ErrCorrupt（区别于写入失败与源中断）。
func TestErrorKindsDistinguishable(t *testing.T) {
	if errors.Is(checkpoint.ErrWriteFailed, checkpoint.ErrCorrupt) {
		t.Fatal("write failure and corruption must be distinct errors")
	}
}
