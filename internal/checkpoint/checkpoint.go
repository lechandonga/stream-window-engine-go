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
	// SeenValues 与 SeenIDs 同序记录每个去重事件的 Value；
	// 旧版本快照缺省，恢复后集合去重语义不受影响。
	SeenValues []int64
	// MaxEventNano 仅会话窗口使用：会话内最晚事件时间（UnixNano），
	// 用于顺序无关地重建窗口 End=MaxEvent+Gap。零值表示非会话/无记录。
	MaxEventNano int64
	// Pending 是该条目（存活会话）尚待在屏障点发出的撤销记录；
	// 正常在同一屏障随快照持久化前已清空，保留字段以确保
	// "投递成功但检查点前崩溃"场景下撤销不丢、不重。
	Pending []RetractionSnapshot
}

// RetractionSnapshot 是一条待发/已发撤销的可持久化形式。
// 它指向被撤销结果最后发布时的完整快照（ResultID+Revision+
// Window+Value+Count），取代者由承载它的存活条目隐式确定。
type RetractionSnapshot struct {
	ResultID string
	Revision int64
	Start    time.Time
	End      time.Time
	Seq      int64 // 会话窗口 Seq
	Value    int64
	Count    int64
	Late     bool
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
