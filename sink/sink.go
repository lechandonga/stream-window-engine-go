package sink

import (
	"errors"
	"sync"
)

// ErrSinkUnavailable 输出端不可用（投递失败；记录仍保留在检查点 Pending 中，恢复后重试）。
var ErrSinkUnavailable = errors.New("sink unavailable")

// Record 是投递给下游的一条输出。
//
// 语义：
//   - IsCorrection=false 为首次输出（窗口第一次触发）；
//   - IsCorrection=true  为修正输出（迟到事件改变了已触发窗口的结果）；
//   - Seq 在一次引擎生命周期内严格递增，重复投递同一 Seq 不得产生重复效果。
type Record struct {
	Seq          int64  `json:"seq"`
	Key          string `json:"key"`
	WindowID     string `json:"window_id"`
	WindowKind   string `json:"window_kind"`
	IsCorrection bool   `json:"is_correction"`
	Value        int64  `json:"value"`
	Reason       string `json:"reason"`
}

// Sink 输出端抽象。Emit 必须对同一 Seq 幂等。
type Sink interface {
	// Emit 投递一条记录；同 Seq 重复调用必须只生效一次并返回 nil。
	Emit(r Record) error
	// LastSeq 返回已确认生效的最大 Seq 与是否存在已投递记录。
	LastSeq() (int64, bool)
}

// MemorySink 内存输出端，按 Seq 去重，主要用于测试。
type MemorySink struct {
	mu       sync.Mutex
	records  []Record
	seen     map[int64]struct{}
	failWith error
}

func NewMemorySink() *MemorySink {
	return &MemorySink{seen: map[int64]struct{}{}}
}

func (s *MemorySink) Emit(r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failWith != nil {
		return s.failWith
	}
	if _, ok := s.seen[r.Seq]; ok {
		// 幂等：重复投递直接视为成功，不重复计数。
		return nil
	}
	s.seen[r.Seq] = struct{}{}
	s.records = append(s.records, r)
	return nil
}

func (s *MemorySink) LastSeq() (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.records) == 0 {
		return 0, false
	}
	return s.records[len(s.records)-1].Seq, true
}

// Records 返回已生效输出的副本。
func (s *MemorySink) Records() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, len(s.records))
	copy(out, s.records)
	return out
}

// SetFail 注入投递失败（恢复 nil 后 Pending 记录可重投）。
func (s *MemorySink) SetFail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failWith = err
}
