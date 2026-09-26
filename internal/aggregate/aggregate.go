// Package aggregate 定义聚合函数、窗口累加状态与可区分新增/修正/撤销的输出。
//
// 输出语义：
//   - Insert：某个逻辑结果（由 ResultID 标识）的首次结果，Revision=1；
//   - Correction：迟到事件在宽限期内纳入后对已发出结果的修正，
//     Revision 严格递增；下游可凭 Seq 幂等、凭 Revision 判断新旧；
//   - Retract：一条此前已发布的结果不再成立（典型场景：两个已发布的
//     会话因迟到事件被合并为一个）。撤销消息通过 ResultID 明确指向
//     被撤销的那条结果，并通过 ReplacedBy 指向取代它的当前结果，
//     下游可据此删除幽灵记录并对账。
package aggregate

import (
	"sort"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// OutputKind 区分首次输出、修正输出与撤销。
type OutputKind int

const (
	// Insert 首次为该逻辑结果发出的结果。
	Insert OutputKind = iota
	// Correction 迟到事件纳入后对既有结果的修正。
	Correction
	// Retract 撤销一条此前已发布、现已不再成立的结果。
	Retract
)

func (k OutputKind) String() string {
	switch k {
	case Insert:
		return "INSERT"
	case Correction:
		return "CORRECTION"
	default:
		return "RETRACT"
	}
}

// Output 是发往下游的单条聚合结果（含撤销）。
// Seq 在单个引擎实例内单调递增，作为全局投递幂等键；
// Revision 为同一逻辑结果的修订号，首版为 1，每次修正 +1。
//
// ResultID 是逻辑结果的稳定标识，只依赖数据（分组键与内容决定的
// 窗口身份），与运行次数、并发度、是否中断恢复无关：
//   - 滚动/滑动窗口：group + 窗口类型 + 固定窗口边界；
//   - 会话窗口：group + 会话内最早事件时间（会话被迟到事件延长或
//     与其他会话合并时保持不变）。
//
// Kind=Retract 时本条是撤销消息：Value/Count/Window 携带被撤销结果
// 最后一次发布时的快照，ReplacedBy 指向取代它的当前结果的 ResultID
// （空串表示纯撤销、无替代）。下游可凭 (ResultID, Revision) 判断
// 撤销是否针对当前持有的版本，重复投递不会重复撤销。
type Output struct {
	Seq        int64         `json:"seq"`
	Revision   int64         `json:"revision"`
	Kind       OutputKind    `json:"kind"`
	ResultID   string        `json:"result_id"`
	Key        string        `json:"key"`
	Window     window.Window `json:"window"`
	Value      int64         `json:"value"`
	Count      int64         `json:"count"`
	ReplacedBy string        `json:"replaced_by,omitempty"`
	EmitAt     time.Time     `json:"emit_at"`
	Watermark  time.Time     `json:"watermark"`
	Late       bool          `json:"late"`
}

// Accumulator 保存单个 (Key, Window) 的聚合中间状态，
// 同时承担该窗口内的事件去重：重复投递的事件 ID 不会二次计数。
// Seen 记录每个已纳入事件 ID 的 Value，用于会话合并时跨成员按 ID
// 去重重算（迟到事件的重复投递可能命中被并入的旧成员）。
type Accumulator struct {
	Value int64
	Count int64
	Seen  map[string]int64
}

// NewAccumulator 创建空累加器。
func NewAccumulator() *Accumulator {
	return &Accumulator{Seen: map[string]int64{}}
}

// Add 纳入一个事件；若 ID 已见过（重复投递）则返回 false 且状态不变。
// 没有 ID 的事件（空串）不去重——调用方应保证输入带 ID。
func (a *Accumulator) Add(e event.Event) bool {
	if e.ID != "" {
		if _, ok := a.Seen[e.ID]; ok {
			return false
		}
		a.Seen[e.ID] = e.Value
	}
	a.Value += e.Value
	a.Count++
	return true
}

// MergeInto 把 other 中尚未在 a 中出现的事件并入 a，返回新纳入的事件数。
// 同 ID 的事件（重复投递，典型为迟到路径）只计一次，Value/Count 与
// Seen 保持一致，保证会话合并的结果确定且不重复计数。
func (a *Accumulator) MergeInto(other *Accumulator) int {
	added := 0
	for id, v := range other.Seen {
		if _, ok := a.Seen[id]; ok {
			continue
		}
		a.Seen[id] = v
		a.Value += v
		a.Count++
		added++
	}
	return added
}

// SeenIDs 以确定性顺序导出去重集合（检查点用）。
func (a *Accumulator) SeenIDs() []string {
	ids := make([]string, 0, len(a.Seen))
	for id := range a.Seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// SeenValues 与 SeenIDs 同序导出各事件的 Value（检查点用）。
func (a *Accumulator) SeenValues() []int64 {
	ids := a.SeenIDs()
	vs := make([]int64, 0, len(ids))
	for _, id := range ids {
		vs = append(vs, a.Seen[id])
	}
	return vs
}

// LoadSeen 从检查点恢复去重集合；values 与 ids 同序，
// 缺省（旧版本检查点）时按 0 恢复，集合语义不受影响。
func (a *Accumulator) LoadSeen(ids []string, values []int64) {
	if a.Seen == nil {
		a.Seen = map[string]int64{}
	}
	for i, id := range ids {
		var v int64
		if i < len(values) {
			v = values[i]
		}
		a.Seen[id] = v
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
