package checkpoint

import (
	"sync"
)

// MemoryStore 内存检查点存储，用于测试。
// 可通过 Corrupt() 模拟检查点损坏，通过 SetSaveError 模拟写入失败。
type MemoryStore struct {
	mu        sync.Mutex
	data      []byte
	exists    bool
	saveError error
	corrupted bool
	saveCount int
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) Save(state *State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveError != nil {
		return s.saveError
	}
	payload, err := jsonMarshalState(state)
	if err != nil {
		return err
	}
	s.data = payload
	s.exists = true
	s.corrupted = false
	s.saveCount++
	return nil
}

func (s *MemoryStore) Load() (*State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists {
		return nil, ErrNoCheckpoint
	}
	if s.corrupted {
		return nil, ErrCorrupt
	}
	return jsonUnmarshalState(s.data)
}

func (s *MemoryStore) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = nil
	s.exists = false
	s.corrupted = false
	return nil
}

// Corrupt 模拟检查点损坏（如磁盘脏数据）。
func (s *MemoryStore) Corrupt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exists = true
	s.corrupted = true
}

// SetSaveError 注入写入失败。
func (s *MemoryStore) SetSaveError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveError = err
}

// SaveCount 返回成功保存的次数，用于断言检查点频率。
func (s *MemoryStore) SaveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveCount
}
