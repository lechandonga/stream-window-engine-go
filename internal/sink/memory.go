package sink

import (
	"sort"
	"sync"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
)

// MemorySink 是进程内确定性输出收集器。
// Deliver 对 Seq 幂等：重启重放或任何重复投递都不会产生重复结果。
type MemorySink struct {
	mu     sync.Mutex
	out    map[int64]aggregate.Output
	order  []int64
	sides  []SideRecord
	closed bool
}

// NewMemory 创建内存输出。
func NewMemory() *MemorySink {
	return &MemorySink{out: map[int64]aggregate.Output{}}
}

// Deliver 幂等投递输出。
func (m *MemorySink) Deliver(o aggregate.Output) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrSink
	}
	if _, exists := m.out[o.Seq]; exists {
		// 重复投递：确认但不重复计数。
		return nil
	}
	m.out[o.Seq] = o
	m.order = append(m.order, o.Seq)
	return nil
}

// DeliverSide 记录被特殊处理的事件（迟到丢弃/侧输出/异常时间）。
func (m *MemorySink) DeliverSide(r SideRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrSink
	}
	m.sides = append(m.sides, r)
	return nil
}

// LastSeq 返回已确认的最大 Seq（无输出时为 0）。
func (m *MemorySink) LastSeq() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var max int64
	for s := range m.out {
		if s > max {
			max = s
		}
	}
	return max
}

// Outputs 按 Seq 升序返回已确认输出（确定性顺序）。
func (m *MemorySink) Outputs() []aggregate.Output {
	m.mu.Lock()
	defer m.mu.Unlock()
	seqs := append([]int64(nil), m.order...)
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	out := make([]aggregate.Output, 0, len(seqs))
	for _, s := range seqs {
		out = append(out, m.out[s])
	}
	return out
}

// Sides 返回全部旁路记录。
func (m *MemorySink) Sides() []SideRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]SideRecord(nil), m.sides...)
}

// Close 关闭输出。
func (m *MemorySink) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
