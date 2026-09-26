package state

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
)

// Config 内存/落盘配置。
type Config struct {
	MaxActiveKeys int            // 内存活动键上限；<=0 表示不限
	Overflow      OverflowPolicy // 上限策略
	SpillDir      string         // 落盘目录（OverflowSpill 时必填）
}

// spillRecord 是落盘条目的磁盘格式（键与状态同存）。
type spillRecord struct {
	Key   Key
	Entry Entry
}

// memoryStore 是带 LRU 淘汰、落盘与确定性遍历的状态存储。
type memoryStore struct {
	cfg  Config
	reg  *aggregate.Registry
	mu   sync.Mutex
	data map[Key]*Entry
	lru  *list.List // 元素为 Key，队首最久未访问
	// key -> LRU 元素，O(1) 移动/删除
	elems    map[Key]*list.Element
	spillCnt int64
	slot     chan struct{} // OverflowBlock 的背压信号
}

// New 创建有界状态存储。
func New(cfg Config) (Store, error) {
	if cfg.Overflow == OverflowSpill {
		if cfg.SpillDir == "" {
			return nil, ErrSpillDirRequired
		}
		if err := os.MkdirAll(cfg.SpillDir, 0o755); err != nil {
			return nil, fmt.Errorf("create spill dir: %w", err)
		}
	}
	s := &memoryStore{
		cfg:   cfg,
		reg:   aggregate.NewRegistry(),
		data:  map[Key]*Entry{},
		lru:   list.New(),
		elems: map[Key]*list.Element{},
		slot:  make(chan struct{}, 1),
	}
	return s, nil
}

func (s *memoryStore) spillPath(k Key) string {
	return filepath.Join(s.cfg.SpillDir, encodeSpillName(k)+".json")
}

// touch 将键标记为最近使用（调用方持锁）。
func (s *memoryStore) touch(k Key) {
	if el, ok := s.elems[k]; ok {
		s.lru.MoveToBack(el)
		return
	}
	s.elems[k] = s.lru.PushBack(k)
}

func (s *memoryStore) removeLRU(k Key) {
	if el, ok := s.elems[k]; ok {
		s.lru.Remove(el)
		delete(s.elems, k)
	}
}

// waitRoom 在 OverflowBlock 模式下等待内存空位，响应 ctx 取消。
// 调用方持锁。
func (s *memoryStore) waitRoom(ctx context.Context, isNew bool) error {
	if !isNew || s.cfg.MaxActiveKeys <= 0 || len(s.data) < s.cfg.MaxActiveKeys {
		return nil
	}
	switch s.cfg.Overflow {
	case OverflowSpill:
		return s.evictLocked()
	default:
		for len(s.data) >= s.cfg.MaxActiveKeys {
			s.mu.Unlock()
			select {
			case <-s.slot:
			case <-ctx.Done():
				s.mu.Lock()
				return ctx.Err()
			}
			s.mu.Lock()
		}
	}
	return nil
}

// evictLocked 淘汰队首冷状态到磁盘（调用方持锁）。
func (s *memoryStore) evictLocked() error {
	if s.lru.Len() == 0 {
		return nil
	}
	victim := s.lru.Remove(s.lru.Front()).(Key)
	delete(s.elems, victim)
	en := s.data[victim]
	delete(s.data, victim)
	return s.writeSpill(victim, en)
}

// writeSpill 原子落盘：临时文件 + rename，杜绝半文件。
func (s *memoryStore) writeSpill(k Key, en *Entry) error {
	rec := spillRecord{Key: k, Entry: *en}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	path := s.spillPath(k)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("write spill temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename spill: %w", err)
	}
	s.spillCnt++
	return nil
}

func (s *memoryStore) readSpill(k Key) (*Entry, error) {
	b, err := os.ReadFile(s.spillPath(k))
	if err != nil {
		return nil, err
	}
	var rec spillRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, fmt.Errorf("decode spill %s: %w", k.ID(), err)
	}
	if rec.Entry.Acc == nil {
		rec.Entry.Acc = aggregate.NewAccumulator()
	}
	return &rec.Entry, nil
}

func (s *memoryStore) GetOrCreate(ctx context.Context, k Key, aggName string) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if en, ok := s.data[k]; ok {
		s.touch(k)
		return en, nil
	}
	// 尝试从磁盘读回。
	if en, err := s.readSpill(k); err == nil {
		if err := s.waitRoom(ctx, true); err != nil {
			return nil, err
		}
		os.Remove(s.spillPath(k))
		s.data[k] = en
		s.touch(k)
		return en, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	agg, ok := s.reg.Get(aggName)
	if !ok {
		return nil, fmt.Errorf("unknown aggregator %q", aggName)
	}
	if err := s.waitRoom(ctx, true); err != nil {
		return nil, err
	}
	en := &Entry{Acc: agg.Init()}
	s.data[k] = en
	s.touch(k)
	return en, nil
}

func (s *memoryStore) Apply(ctx context.Context, k Key, e event.Event) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	en, ok := s.data[k]
	if !ok {
		// 可能在磁盘上。
		if loaded, err := s.readSpill(k); err == nil {
			if err := s.waitRoom(ctx, true); err != nil {
				return false, err
			}
			os.Remove(s.spillPath(k))
			en = loaded
			s.data[k] = en
			s.touch(k)
		} else if !os.IsNotExist(err) {
			return false, err
		} else {
			// Apply 前通常已 GetOrCreate；走到这里按默认 sum_count 创建。
			if err := s.waitRoom(ctx, true); err != nil {
				return false, err
			}
			en = &Entry{Acc: aggregate.NewAccumulator()}
			s.data[k] = en
			s.touch(k)
		}
	} else {
		s.touch(k)
	}
	added := en.Acc.Add(e)
	return added, nil
}

func (s *memoryStore) Get(ctx context.Context, k Key) (*Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if en, ok := s.data[k]; ok {
		s.touch(k)
		return en, true, nil
	}
	if en, err := s.readSpill(k); err == nil {
		if err := s.waitRoom(ctx, true); err != nil {
			return nil, false, err
		}
		os.Remove(s.spillPath(k))
		s.data[k] = en
		s.touch(k)
		return en, true, nil
	} else if !os.IsNotExist(err) {
		// 损坏的 spill 文件必须被当作失败暴露，不静默丢弃。
		return nil, false, fmt.Errorf("read spill: %w", err)
	}
	return nil, false, nil
}

func (s *memoryStore) Put(k Key, en *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[k] = en
	s.touch(k)
	return nil
}

func (s *memoryStore) Delete(k Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, k)
	s.removeLRU(k)
	if s.cfg.Overflow == OverflowSpill {
		if err := os.Remove(s.spillPath(k)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	// 唤醒一个等待空位的写入者。
	select {
	case s.slot <- struct{}{}:
	default:
	}
	return nil
}

func (s *memoryStore) MarkEmitted(k Key, revision int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if en, ok := s.data[k]; ok {
		en.Emitted = true
		en.Revision = revision
		s.touch(k)
		return nil
	}
	// 已落盘：读出改完写回。
	en, err := s.readSpill(k)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("mark emitted: key %s not found", k.ID())
		}
		return err
	}
	en.Emitted = true
	en.Revision = revision
	return s.writeSpill(k, en)
}

func (s *memoryStore) Close(k Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if en, ok := s.data[k]; ok {
		en.Closed = true
		return nil
	}
	en, err := s.readSpill(k)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("close: key %s not found", k.ID())
		}
		return err
	}
	en.Closed = true
	return s.writeSpill(k, en)
}

// Purge 清理 End 早于 purgeBefore、已关闭且已发出的窗口。
// 引擎不变量：窗口只有在 watermark 越过 End+AllowedLateness 时才被 Close，
// 而它早在 watermark>=End 时就已 Emitted，因此 Closed==true 蕴含
// Emitted==true；这里仍显式检查 Emitted，作为防御性断言。
// 允许迟到宽限由引擎换算为 purgeBefore 后传入。
func (s *memoryStore) Purge(purgeBeforeUnixNano int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, en := range s.data {
		if en.Closed && en.Emitted && k.Win.End.UnixNano() < purgeBeforeUnixNano {
			delete(s.data, k)
			s.removeLRU(k)
			n++
		}
	}
	// 落盘条目同样清理。
	if s.cfg.Overflow == OverflowSpill {
		spilled, err := s.listSpillLocked()
		if err != nil {
			return n, err
		}
		for _, rec := range spilled {
			if rec.Entry.Closed && rec.Entry.Emitted && rec.Key.Win.End.UnixNano() < purgeBeforeUnixNano {
				if err := os.Remove(s.spillPath(rec.Key)); err != nil && !os.IsNotExist(err) {
					return n, err
				}
				n++
			}
		}
	}
	for i := 0; i < n; i++ {
		select {
		case s.slot <- struct{}{}:
		default:
		}
	}
	return n, nil
}

// listSpillLocked 读取并解码全部落盘条目（调用方持锁）。
func (s *memoryStore) listSpillLocked() ([]spillRecord, error) {
	entries, err := os.ReadDir(s.cfg.SpillDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []spillRecord
	for _, f := range entries {
		if f.IsDir() || filepath.Ext(f.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.cfg.SpillDir, f.Name()))
		if err != nil {
			return nil, err
		}
		var rec spillRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			// 损坏的落盘文件必须暴露为失败。
			return nil, fmt.Errorf("corrupt spill file %s: %w", f.Name(), err)
		}
		out = append(out, rec)
	}
	return out, nil
}

func (s *memoryStore) ListKeys(ctx context.Context) ([]Key, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]Key, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	if s.cfg.Overflow == OverflowSpill {
		spilled, err := s.listSpillLocked()
		if err != nil {
			return nil, err
		}
		exist := make(map[Key]struct{}, len(keys))
		for _, k := range keys {
			exist[k] = struct{}{}
		}
		for _, rec := range spilled {
			if _, ok := exist[rec.Key]; !ok {
				keys = append(keys, rec.Key)
			}
		}
	}
	sortKeys(keys)
	return keys, nil
}

func sortKeys(keys []Key) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Group != keys[j].Group {
			return keys[i].Group < keys[j].Group
		}
		a, b := keys[i].Win, keys[j].Win
		if !a.Start.Equal(b.Start) {
			return a.Start.Before(b.Start)
		}
		if !a.End.Equal(b.End) {
			return a.End.Before(b.End)
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Seq < b.Seq
	})
}

func (s *memoryStore) SpillFiles() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.Overflow != OverflowSpill {
		return nil
	}
	entries, err := os.ReadDir(s.cfg.SpillDir)
	if err != nil {
		return nil
	}
	var names []string
	for _, f := range entries {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".json" {
			names = append(names, f.Name())
		}
	}
	sort.Strings(names)
	return names
}

// SnapshotEntries 导出全部条目（内存 + 落盘），不改变布局。
func (s *memoryStore) SnapshotEntries(ctx context.Context) ([]SnapshotEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []SnapshotEntry
	for k, en := range s.data {
		cp := *en
		if cp.Acc != nil {
			a := *en.Acc
			cp.Acc = &a
		}
		out = append(out, SnapshotEntry{K: k, En: cp})
	}
	if s.cfg.Overflow == OverflowSpill {
		spilled, err := s.listSpillLocked()
		if err != nil {
			return nil, err
		}
		exist := make(map[Key]struct{}, len(s.data))
		for k := range s.data {
			exist[k] = struct{}{}
		}
		for _, rec := range spilled {
			if _, ok := exist[rec.Key]; ok {
				continue
			}
			cp := rec.Entry
			if cp.Acc != nil {
				a := *rec.Entry.Acc
				cp.Acc = &a
			}
			out = append(out, SnapshotEntry{K: rec.Key, En: cp})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		ks := []Key{out[i].K, out[j].K}
		sortKeys(ks)
		return ks[0] == out[i].K
	})
	return out, nil
}

// RestoreEntries 用检查点内容重建存储。
// 全部先重建到内存，超出 MaxActiveKeys 时按 LRU 落盘，
// 因此恢复后的布局也是确定的（按确定性键序写入、逆序淘汰）。
func (s *memoryStore) RestoreEntries(ctx context.Context, entries []SnapshotEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		en := e.En
		if en.Acc == nil {
			en.Acc = aggregate.NewAccumulator()
		} else if en.Acc.Seen == nil {
			en.Acc.Seen = map[string]int64{}
		}
		if s.cfg.MaxActiveKeys > 0 && len(s.data) >= s.cfg.MaxActiveKeys {
			if s.cfg.Overflow == OverflowSpill {
				if err := s.evictLocked(); err != nil {
					return err
				}
			}
		}
		s.data[e.K] = &en
		s.touch(e.K)
	}
	return nil
}

func (s *memoryStore) ActiveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

func (s *memoryStore) SpillCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spillCnt
}

func (s *memoryStore) CloseAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.Overflow == OverflowSpill {
		// 退出前把内存状态全部落盘，保证 Close 后状态不丢。
		for k, en := range s.data {
			if err := s.writeSpill(k, en); err != nil {
				return err
			}
		}
	}
	return nil
}
