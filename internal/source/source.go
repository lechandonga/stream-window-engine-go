// Package source 定义事件源：可中断检测、可回放、带单调偏移量。
//
// MemorySource 以内存切片模拟事件源：偏移量从 0 单调分配，
// 可配置在投递 N 条后返回 ErrInterrupted 模拟来源中断；
// 从指定 offset 重新 Subscribe 即可确定性回放，重复回放得到完全相同的序列。
package source

import (
	"context"
	"errors"

	"github.com/lechandonga/stream-window-engine-go/internal/event"
)

// ErrInterrupted 表示来源中断（与检查点失败明确区分）。
var ErrInterrupted = errors.New("source interrupted")

// Record 是带偏移量的事件信封。
type Record struct {
	Offset int64
	Event  event.Event
}

// Source 是可重放事件源。
type Source interface {
	// Subscribe 从 offset（含）开始读取。
	// 数据通道关闭后，Err() 返回结束原因：
	// nil=正常读完；ErrInterrupted=来源中断；context 错误=被取消。
	Subscribe(ctx context.Context, offset int64) (<-chan Record, error)
	// Err 返回最近一次订阅的结束原因（在数据通道关闭后读取）。
	Err() error
	// Size 返回事件总数（回放一致性校验用）。
	Size() int
}

// MemorySource 以内存切片模拟事件源，可制造中断。
type MemorySource struct {
	records    []Record
	failAfterN int // 投递 N 条后返回 ErrInterrupted；<=0 不中断
	mu         struct {
		err error
	}
}

// NewMemory 从事件列表构造内存源（偏移量从 0 单调分配）。
func NewMemory(events []event.Event) *MemorySource {
	recs := make([]Record, len(events))
	for i, e := range events {
		recs[i] = Record{Offset: int64(i), Event: e}
	}
	return &MemorySource{records: recs, failAfterN: -1}
}

// NewMemoryWithFailure 构造在投递 failAfterN 条后中断的源。
func NewMemoryWithFailure(events []event.Event, failAfterN int) *MemorySource {
	m := NewMemory(events)
	m.failAfterN = failAfterN
	return m
}

// Subscribe 实现 Source，从 offset 起顺序投递。
// offset == len(records) 合法（恢复时源已耗尽，立即正常结束）；
// offset 超出本源长度时容错钳制到末尾（重启时使用子集源的场景）。
func (m *MemorySource) Subscribe(ctx context.Context, offset int64) (<-chan Record, error) {
	if offset < 0 {
		return nil, errors.New("subscribe offset negative")
	}
	if offset > int64(len(m.records)) {
		offset = int64(len(m.records))
	}
	m.mu.err = nil
	ch := make(chan Record)
	go func() {
		defer close(ch)
		delivered := 0
		for i := offset; i < int64(len(m.records)); i++ {
			if m.failAfterN > 0 && delivered >= m.failAfterN {
				m.mu.err = ErrInterrupted
				return
			}
			select {
			case <-ctx.Done():
				m.mu.err = ctx.Err()
				return
			case ch <- m.records[i]:
				delivered++
			}
		}
	}()
	return ch, nil
}

// Err 返回最近一次订阅的结束原因。
func (m *MemorySource) Err() error { return m.mu.err }

// Size 实现 Source。
func (m *MemorySource) Size() int { return len(m.records) }
