// Package aggregate 定义聚合函数、窗口累加状态与可区分新增/修正的输出。
//
// 输出语义：
//   - Insert：某个 (Key, Window) 的首次结果，Revision=1；
//   - Correction：迟到事件在宽限期内纳入后对已发出结果的修正，
//     Revision 严格递增；下游可凭 Seq 幂等、凭 Revision 判断新旧。
package aggregate

import (
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// OutputKind 区分首次输出与修正输出。
type OutputKind int

const (
	// Insert 首次为该 (Key, Window) 发出的结果。
	Insert OutputKind = iota
	// Correction 迟到事件纳入后对既有结果的修正。
	Correction
)

func (k OutputKind) String() string {
	if k == Insert {
		return "INSERT"
	}
	return "CORRECTION"
}

// Output 是发往下游的单条聚合结果。
// Seq 在单个引擎实例内单调递增，作为全局幂等键；
// Revision 为同一 (Key, Window) 的修订号，首版为 1，每次修正 +1。
type Output struct {
	Seq       int64         `json:"seq"`
	Revision  int64         `json:"revision"`
	Kind      OutputKind    `json:"kind"`
	Key       string        `json:"key"`
	Window    window.Window `json:"window"`
	Value     int64         `json:"value"`
	Count     int64         `json:"count"`
	EmitAt    time.Time     `json:"emit_at"`
	Watermark time.Time     `json:"watermark"`
	Late      bool          `json:"late"`
}

// Accumulator 保存单个 (Key, Window) 的聚合中间状态，
// 同时承担该窗口内的事件去重：重复投递的事件 ID 不会二次计数。
type Accumulator struct {
	Value int64
	Count int64
	Seen  map[string]struct{}
}

// NewAccumulator 创建空累加器。
func NewAccumulator() *Accumulator {
	return &Accumulator{Seen: map[string]struct{}{}}
}

// Add 纳入一个事件；若 ID 已见过（重复投递）则返回 false 且状态不变。
// 没有 ID 的事件（空串）不去重——调用方应保证输入带 ID。
func (a *Accumulator) Add(e event.Event) bool {
	if e.ID != "" {
		if _, ok := a.Seen[e.ID]; ok {
			return false
		}
		a.Seen[e.ID] = struct{}{}
	}
	a.Value += e.Value
	a.Count++
	return true
}

// SeenIDs 以确定性顺序导出去重集合（检查点用）。
func (a *Accumulator) SeenIDs() []string {
	ids := make([]string, 0, len(a.Seen))
	for id := range a.Seen {
		ids = append(ids, id)
	}
	return ids
}

// LoadSeen 从检查点恢复去重集合。
func (a *Accumulator) LoadSeen(ids []string) {
	if a.Seen == nil {
		a.Seen = map[string]struct{}{}
	}
	for _, id := range ids {
		a.Seen[id] = struct{}{}
	}
}

// Aggregator 是聚合函数抽象。
type Aggregator interface {
	Name() string
	Init() *Accumulator
}

// SumCount 同时计算 Value 之和与事件数。
type SumCount struct{}

// Name 返回聚合器名称。
func (SumCount) Name() string { return "sum_count" }

// Init 创建累加器。
func (SumCount) Init() *Accumulator { return NewAccumulator() }
