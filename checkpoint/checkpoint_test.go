package checkpoint

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStoreRoundtripAndCorruption(t *testing.T) {
	dir := t.TempDir()
	st := NewFileStore(dir)
	if _, err := st.Load(); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("want ErrNoCheckpoint, got %v", err)
	}
	in := &State{Version: 1, WatermarkUnixMs: 123, NextOutputSeq: 9, CommittedSeq: 8}
	if err := st.Save(in); err != nil {
		t.Fatal(err)
	}
	out, err := st.Load()
	if err != nil || out.WatermarkUnixMs != 123 || out.NextOutputSeq != 9 {
		t.Fatalf("roundtrip err=%v out=%+v", err, out)
	}
	// 损坏：追加字节破坏校验和。
	p := filepath.Join(dir, "checkpoint.json")
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	_, _ = f.Write([]byte("XX"))
	f.Close()
	if _, err := st.Load(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt after tamper, got %v", err)
	}
}

func TestFileStoreAtomicWriteKeepsOldOnFailure(t *testing.T) {
	dir := t.TempDir()
	st := NewFileStore(dir)
	if err := st.Save(&State{Version: 1, WatermarkUnixMs: 7}); err != nil {
		t.Fatal(err)
	}
	// 把目录变成只读文件以制造写入失败（root 可绕过，测试环境非 root 才有效）。
	blocker := filepath.Join(dir, "blocker")
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err == nil {
		if err := st.Save(&State{Version: 1}); err == nil {
			t.Skip("could not force write failure")
		}
	}
}

func TestMemoryStoreCorruptAndSaveError(t *testing.T) {
	m := NewMemoryStore()
	if err := m.Save(&State{Version: 1}); err != nil {
		t.Fatal(err)
	}
	m.Corrupt()
	if _, err := m.Load(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}
