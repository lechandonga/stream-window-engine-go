// Package checkpoint 实现本地原子检查点：写入、损坏检测与恢复。
package checkpoint

import (
	"errors"
	"time"
)

// ErrCorrupt 表示检查点内容损坏（校验和不匹配/截断/魔数错误）。
var ErrCorrupt = errors.New("checkpoint corrupt")

// ErrWriteFailed 表示检查点写入失败（调用方可与源中断区分）。
var ErrWriteFailed = errors.New("checkpoint write failed")

// Snapshot 是一次一致性检查点的完整内容。
type Snapshot struct {
	Version      int
	Watermark    time.Time
	NextSeq      int64
	SourceOffset int64
	Entries      []EntrySnapshot
	SpillFiles   []string
	CreatedAt    time.Time
}

// EntrySnapshot 是单个窗口状态的可持久化形式。
type EntrySnapshot struct {
	Group    string
	Kind     int
	Start    time.Time
	End      time.Time
	Seq      int64
	Value    int64
	Count    int64
	Revision int64
	Emitted  bool
	Closed   bool
	SeenIDs  []string
}

// Store 抽象检查点存储介质（本地文件系统实现）。
type Store interface {
	// Save 原子写入：先写临时文件 + 校验和，再 rename；失败不留半成品。
	Save(snap Snapshot) error
	// Load 读取最新检查点；不存在时返回 ErrNotFound。
	Load() (Snapshot, error)
	// Clear 删除全部检查点数据。
	Clear() error
}

// ErrNotFound 表示尚无检查点。
var ErrNotFound = errors.New("checkpoint not found")
