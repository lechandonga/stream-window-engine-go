package checkpoint

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sample() Snapshot {
	return Snapshot{
		Version: 1, Watermark: time.Unix(100, 0), NextSeq: 7, SourceOffset: 9,
		Entries: []EntrySnapshot{
			{Group: "k", Start: time.Unix(0, 0), End: time.Unix(10, 0), Value: 3, Count: 1, Revision: 1, Emitted: true, SeenIDs: []string{"a"}},
		},
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.Save(sample()); err != nil {
		t.Fatal(err)
	}
	got, err := fs.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.NextSeq != 7 || got.SourceOffset != 9 || !got.Watermark.Equal(time.Unix(100, 0)) {
		t.Fatalf("snapshot mismatch: %+v", got)
	}
	if len(got.Entries) != 1 || got.Entries[0].Value != 3 || len(got.Entries[0].SeenIDs) != 1 {
		t.Fatalf("entries mismatch: %+v", got.Entries)
	}
	// 不得残留临时文件。
	if _, err := os.Stat(filepath.Join(dir, "chk.json.tmp")); !os.IsNotExist(err) {
		t.Fatal("temp file left behind")
	}
}

func TestLoadMissing(t *testing.T) {
	fs, _ := NewFileStore(t.TempDir())
	if _, err := fs.Load(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestCorruptDetected(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStore(dir)
	if err := fs.Save(sample()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "chk.json")

	corrupt := func(mutate func(b []byte) []byte) {
		b, _ := os.ReadFile(path)
		os.WriteFile(path, mutate(b), 0o644)
		fs2, _ := NewFileStore(dir)
		if _, err := fs2.Load(); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	}

	// 1) 截断。
	corrupt(func(b []byte) []byte { return b[:len(b)/4] })
	// 2) 篡改校验和值：翻转校验和首字符（0<->1，其它->0），必然与原值不同。
	_ = fs.Save(sample())
	corrupt(func(b []byte) []byte {
		s := string(b)
		idx := indexAfter(s, `"sha256":"`)
		if idx < 0 {
			t.Fatal("sha256 field not found")
		}
		bb := []byte(s)
		switch bb[idx] {
		case '0':
			bb[idx] = '1'
		default:
			bb[idx] = '0'
		}
		return bb
	})
	// 3) 载荷被改但校验和未变（载荷是转义存储的 JSON 字符串）。
	_ = fs.Save(sample())
	corrupt(func(b []byte) []byte {
		s := strings.ReplaceAll(string(b), `\"NextSeq\":7`, `\"NextSeq\":8`)
		return []byte(s)
	})
	// 4) 垃圾内容。
	_ = fs.Save(sample())
	corrupt(func(b []byte) []byte { return []byte("not-json-at-all{") })
}

func indexAfter(s, sub string) int {
	i := strings.Index(s, sub)
	if i < 0 {
		return -1
	}
	return i + len(sub)
}

func TestSaveFailureNoHalfState(t *testing.T) {
	// 指向文件而非目录 -> 保存失败，且不得 panic；旧快照不受影响。
	base := t.TempDir()
	blocker := filepath.Join(base, "afile")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileStore(blocker); err == nil {
		t.Fatal("expected NewFileStore to fail on file path")
	} else if !errors.Is(err, ErrWriteFailed) {
		t.Fatalf("want ErrWriteFailed, got %v", err)
	}
}
