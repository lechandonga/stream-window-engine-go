// Package state 提供有界的按键/窗口聚合状态存储。
//
// 上限策略：
//   - OverflowBlock：内存键数达到 MaxActiveKeys 时，新键的写入阻塞等待，
//     直到 Purge 清理彻底过期窗口释放空间（背压）；
//   - OverflowSpill：把最久未访问（LRU）的冷状态原子落盘，读回时透明加载，
//     磁盘文件由检查点统一登记，重启后仍可读回。
//
// 并发安全；遍历键使用确定性顺序（Group、Start、End、Seq），
// 从而保证并发分组处理与串行处理得到一致结果。
package state

import (
	"context"
	"errors"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// Key 是分组键 + 窗口的复合状态键。
type Key struct {
	Group string
	Win   window.Window
}

// ID 返回跨分组唯一的稳定标识（落盘文件名/日志使用）。
func (k Key) ID() string { return k.Group + "|" + window.WindowID(k.Win) }

// Entry 是一个窗口的完整可变状态。
type Entry struct {
	Acc      *aggregate.Accumulator
	Revision int64 // 已发出修订号（0=尚未发出）
	Emitted  bool
	Closed   bool // 窗口已触发且宽限期结束
	Dirty    bool // 自上次发出后有（迟到）更新，下一次触发为 CORRECTION
}

// OverflowPolicy 达到内存上限时的策略。
type OverflowPolicy int

const (
	// OverflowBlock 背压：阻塞调用方直到有状态被清理。
	OverflowBlock OverflowPolicy = iota
	// OverflowSpill 将最久未访问的冷状态落盘，读回时透明加载。
	OverflowSpill
)

// ErrSpillDirRequired 落盘策略缺少目录配置。
var ErrSpillDirRequired = errors.New("spill directory required under OverflowSpill")

// Store 是窗口状态存储。
type Store interface {
	// Apply 向 Key 对应窗口写入一个事件；重复 ID 返回 false 且状态不变。
	Apply(ctx context.Context, k Key, e event.Event) (bool, error)
	// Get 读取状态（落盘策略下可能触发读回）。
	Get(ctx context.Context, k Key) (*Entry, bool, error)
	// GetOrCreate 读取或创建状态条目（聚合器名决定初始累加器）。
	GetOrCreate(ctx context.Context, k Key, aggName string) (*Entry, error)
	// Put 写回一个完整条目（会话合并/恢复时使用）。
	Put(k Key, en *Entry) error
	// Delete 删除一个键及其可能存在的落盘文件。
	Delete(k Key) error
	// ListKeys 以确定性顺序列出全部键（内存+落盘）。
	ListKeys(ctx context.Context) ([]Key, error)
	// MarkEmitted 记录窗口已发出及其修订号。
	MarkEmitted(k Key, revision int64) error
	// Close 标记窗口关闭（水位线越过窗口末端）。
	Close(k Key) error
	// Purge 清理 End < beforeUnixNano 且已关闭的窗口，返回被清理键数。
	Purge(beforeUnixNano int64) (int, error)
	// SpillFiles 返回当前全部落盘文件相对名（检查点用）。
	SpillFiles() []string
	// SnapshotEntries 导出全部条目（含已落盘），不改变内存/落盘布局。
	SnapshotEntries(ctx context.Context) ([]SnapshotEntry, error)
	// RestoreEntries 用检查点内容重建存储（仅在空存储上调用）。
	RestoreEntries(ctx context.Context, entries []SnapshotEntry) error
	// ActiveCount 返回内存中活动状态条目数。
	ActiveCount() int
	// SpillCount 返回累计落盘次数（可观测性）。
	SpillCount() int64
	CloseAll() error
}

// SnapshotEntry 是状态条目的可持久化投影。
type SnapshotEntry struct {
	K  Key
	En Entry
}
