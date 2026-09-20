package source

import (
	"errors"
	"time"

	"github.com/lechandonga/stream-window-engine-go/event"
)

// ErrSourceInterrupted 表示来源中断（需与其它失败原因可区分）。
var ErrSourceInterrupted = errors.New("event source interrupted")

// EventBatch 是从源拉取的一批事件及源侧观测到的进度时间（零值表示无进度信号）。
type EventBatch struct {
	Events   []event.Event
	Progress time.Time
}

// Source 事件源抽象。
type Source interface {
	// Poll 返回下一批事件；io.EOF 表示源正常结束；
	// ErrSourceInterrupted 表示来源异常中断，调用方必须停止处理并报错。
	Poll() (EventBatch, error)
}

// SliceSource 内存事件源，用于测试与演示。
// 可在任意位置 MarkInterrupted 模拟来源中断。
type SliceSource struct {
	batches     []EventBatch
	idx         int
	interrupted bool
	exhausted   bool
}

// NewSliceSource 基于给定批次创建内存源。
func NewSliceSource(batches []EventBatch) *SliceSource {
	return &SliceSource{batches: batches}
}

func (s *SliceSource) Poll() (EventBatch, error) {
	if s.interrupted {
		return EventBatch{}, ErrSourceInterrupted
	}
	if s.idx >= len(s.batches) {
		s.exhausted = true
		return EventBatch{}, errEOF
	}
	b := s.batches[s.idx]
	s.idx++
	return b, nil
}

// MarkInterrupted 标记来源中断，后续 Poll 返回 ErrSourceInterrupted。
func (s *SliceSource) MarkInterrupted() { s.interrupted = true }

// Exhausted 返回源是否已正常消费完毕。
func (s *SliceSource) Exhausted() bool { return s.exhausted }
