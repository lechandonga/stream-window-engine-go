package checkpoint

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// 文件格式（单文件、自描述、可检测损坏）：
//
//	{"format":"swe-cp","version":1,"sha256":"<hex>","payload":"<base64 of canonical state JSON>"}
//
// 任何截断 / 字段缺失 / 校验和不匹配都归类为 ErrCorrupt；
// 磁盘或 fsync 错误归类为 ErrWriteFailed，二者可区分。

const (
	ckptFileName  = "checkpoint.json"
	formatName    = "swe-cp"
	formatVersion = 1
)

type envelope struct {
	Format  string `json:"format"`
	Version int    `json:"version"`
	SHA256  string `json:"sha256"`
	Payload string `json:"payload"`
}

// FileStore 基于本地目录的原子检查点存储。
type FileStore struct {
	dir string
}

func NewFileStore(dir string) *FileStore {
	return &FileStore{dir: dir}
}

func (s *FileStore) path() string { return filepath.Join(s.dir, ckptFileName) }

// Save 原子写入：同目录临时文件 -> fsync 文件 -> rename -> fsync 目录。
// 中途任一步失败都不会覆盖已有检查点（不存在半更新状态）。
func (s *FileStore) Save(state *State) (err error) {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("%w: mkdir %s: %v", ErrWriteFailed, s.dir, err)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("%w: encode state: %v", ErrWriteFailed, err)
	}
	sum := sha256.Sum256(payload)
	env, err := json.Marshal(envelope{
		Format:  formatName,
		Version: formatVersion,
		SHA256:  hex.EncodeToString(sum[:]),
		Payload: base64.StdEncoding.EncodeToString(payload),
	})
	if err != nil {
		return fmt.Errorf("%w: encode envelope: %v", ErrWriteFailed, err)
	}

	tmp, err := os.CreateTemp(s.dir, ".ckpt-*")
	if err != nil {
		return fmt.Errorf("%w: create temp: %v", ErrWriteFailed, err)
	}
	tmpName := tmp.Name()
	// 失败时清理临时文件；已 rename 的情况 Remove 不存在的文件无副作用。
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()
	if _, err = tmp.Write(env); err != nil {
		return fmt.Errorf("%w: write temp: %v", ErrWriteFailed, err)
	}
	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("%w: fsync temp: %v", ErrWriteFailed, err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("%w: close temp: %v", ErrWriteFailed, err)
	}
	if err = os.Rename(tmpName, s.path()); err != nil {
		return fmt.Errorf("%w: rename: %v", ErrWriteFailed, err)
	}
	// fsync 目录以保证 rename 持久化。
	dir, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("%w: open dir for fsync: %v", ErrWriteFailed, err)
	}
	if err = dir.Sync(); err != nil {
		dir.Close()
		return fmt.Errorf("%w: fsync dir: %v", ErrWriteFailed, err)
	}
	if err = dir.Close(); err != nil {
		return fmt.Errorf("%w: close dir: %v", ErrWriteFailed, err)
	}
	return nil
}

// Load 读取并校验检查点。文件不存在返回 ErrNoCheckpoint；
// 任何格式/截断/校验和问题返回 ErrCorrupt（可与写入失败区分）。
func (s *FileStore) Load() (*State, error) {
	raw, err := os.ReadFile(s.path())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNoCheckpoint
		}
		return nil, fmt.Errorf("read checkpoint: %v", err)
	}
	return decodeEnvelope(raw)
}

func decodeEnvelope(raw []byte) (*State, error) {
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("%w: invalid envelope json: %v", ErrCorrupt, err)
	}
	if env.Format != formatName || env.Version != formatVersion || env.Payload == "" || env.SHA256 == "" {
		return nil, fmt.Errorf("%w: unexpected envelope fields format=%q version=%d", ErrCorrupt, env.Format, env.Version)
	}
	payload, err := base64.StdEncoding.DecodeString(env.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: payload base64: %v", ErrCorrupt, err)
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != env.SHA256 {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	var st State
	if err := json.Unmarshal(payload, &st); err != nil {
		return nil, fmt.Errorf("%w: invalid state json: %v", ErrCorrupt, err)
	}
	return &st, nil
}

// Clear 删除检查点文件（文件不存在不视为错误）。
func (s *FileStore) Clear() error {
	if err := os.Remove(s.path()); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: remove: %v", ErrWriteFailed, err)
	}
	return nil
}
