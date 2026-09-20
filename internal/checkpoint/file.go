package checkpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// 文件布局（全部位于检查点目录内）：
//   chk.json      —— 最新快照（JSON，首行载荷、末行 "sha256=<hex>"）
//   chk.json.tmp  —— 写入过程中的临时文件，rename 成功后才生效
//
// 写入是原子的：只有 rename 完成后快照才可见；任何中途失败
// 都不会产生半更新状态，旧快照保持有效。

// FileStore 基于目录的原子检查点存储。
type FileStore struct {
	dir  string
	path string
}

// NewFileStore 创建文件检查点存储（目录不存在则创建）。
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty checkpoint dir", ErrWriteFailed)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrWriteFailed, err)
	}
	return &FileStore{dir: dir, path: filepath.Join(dir, "chk.json")}, nil
}

// envelope 包装载荷、魔数与校验和。
// payload 以字符串保存紧凑 JSON：不能使用 json.RawMessage 配合
// MarshalIndent——缩进编码会重排 Raw 字节，导致校验和失效。
type envelope struct {
	Magic   string `json:"magic"`
	Payload string `json:"payload"`
	SHA256  string `json:"sha256"`
}

const magic = "swe-chk-v1"

// Save 原子保存快照：编码 -> 写临时文件 -> fsync -> rename。
func (f *FileStore) Save(snap Snapshot) error {
	payload, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("%w: encode: %v", ErrWriteFailed, err)
	}
	sum := sha256.Sum256(payload)
	env := envelope{
		Magic:   magic,
		Payload: string(payload),
		SHA256:  hex.EncodeToString(sum[:]),
	}
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("%w: encode envelope: %v", ErrWriteFailed, err)
	}

	tmp := f.path + ".tmp"
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("%w: open temp: %v", ErrWriteFailed, err)
	}
	if _, err := fh.Write(body); err != nil {
		fh.Close()
		os.Remove(tmp)
		return fmt.Errorf("%w: write: %v", ErrWriteFailed, err)
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		os.Remove(tmp)
		return fmt.Errorf("%w: fsync: %v", ErrWriteFailed, err)
	}
	if err := fh.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("%w: close: %v", ErrWriteFailed, err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("%w: rename: %v", ErrWriteFailed, err)
	}
	return nil
}

// Load 加载并严格校验最新快照；魔数/JSON/校验和任一不符都返回 ErrCorrupt。
func (f *FileStore) Load() (Snapshot, error) {
	var snap Snapshot
	body, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return snap, ErrNotFound
		}
		return snap, fmt.Errorf("read checkpoint: %w", err)
	}
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return snap, fmt.Errorf("%w: unmarshal envelope: %v", ErrCorrupt, err)
	}
	if env.Magic != magic {
		return snap, fmt.Errorf("%w: bad magic %q", ErrCorrupt, env.Magic)
	}
	sum := sha256.Sum256([]byte(env.Payload))
	if got := hex.EncodeToString(sum[:]); got != env.SHA256 {
		return snap, fmt.Errorf("%w: checksum mismatch want=%s got=%s", ErrCorrupt, env.SHA256, got)
	}
	if err := json.Unmarshal([]byte(env.Payload), &snap); err != nil {
		return snap, fmt.Errorf("%w: unmarshal payload: %v", ErrCorrupt, err)
	}
	if snap.Version != 1 {
		return snap, fmt.Errorf("%w: unsupported version %d", ErrCorrupt, snap.Version)
	}
	// 条目顺序归一，恢复后行为确定。
	sort.SliceStable(snap.Entries, func(i, j int) bool {
		if snap.Entries[i].Group != snap.Entries[j].Group {
			return snap.Entries[i].Group < snap.Entries[j].Group
		}
		if !snap.Entries[i].Start.Equal(snap.Entries[j].Start) {
			return snap.Entries[i].Start.Before(snap.Entries[j].Start)
		}
		return snap.Entries[i].End.Before(snap.Entries[j].End)
	})
	return snap, nil
}

// Clear 清除检查点目录内容。
func (f *FileStore) Clear() error {
	if err := os.RemoveAll(f.dir); err != nil {
		return fmt.Errorf("%w: %v", ErrWriteFailed, err)
	}
	return os.MkdirAll(f.dir, 0o755)
}
