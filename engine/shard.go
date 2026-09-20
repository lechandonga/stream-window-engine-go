package engine

import (
	"sort"

	"github.com/lechandonga/stream-window-engine-go/window"
)

// liveWindowCount 返回内存中非占位的窗口数（占位符不计入内存占用）。
func (s *shard) liveWindowCount() int {
	n := 0
	for _, ks := range s.keys {
		for _, st := range ks.windows {
			if st.spilledRef == "" {
				n++
			}
		}
	}
	return n
}

// getOrLoad 返回窗口状态；若为落盘占位符则惰性加载（会话窗口需要边界外的完整状态）。
func (s *shard) getOrLoad(key string, ks *keyState, id string, win window.Window, wk window.Kind) (*windowState, error) {
	st, ok := ks.windows[id]
	if !ok {
		return nil, nil
	}
	if st.spilledRef != "" {
		loaded, err := s.engine.spill.loadWindow(id, st.spilledRef, wk)
		if err != nil {
			return nil, err
		}
		s.engine.spill.unregister(key, id, wk)
		// 占位符上保存的边界优先（会话结束时间可能已延长）。
		if !st.w.Start.IsZero() {
			loaded.w = st.w
		} else if loaded.w.Start.IsZero() {
			loaded.w = win
		}
		loaded.fired = st.fired
		loaded.lastSeq = st.lastSeq
		loaded.lastValue = st.lastValue
		loaded.spilledRef = ""
		ks.windows[id] = loaded
		return loaded, nil
	}
	return st, nil
}

// materializeKey 加载一个 key 下全部落盘窗口（会话合并前使用）。
func (s *shard) materializeKey(key string) error {
	ks := s.keys[key]
	if ks == nil {
		return nil
	}
	for id, st := range ks.windows {
		if st.spilledRef == "" {
			continue
		}
		loaded, err := s.engine.spill.loadWindow(id, st.spilledRef, window.Session)
		if err != nil {
			return err
		}
		loaded.w = st.w
		loaded.fired = st.fired
		loaded.lastSeq = st.lastSeq
		loaded.lastValue = st.lastValue
		loaded.spilledRef = ""
		ks.windows[id] = loaded
	}
	return nil
}

// reserveCapacity 预判本批次需要新建的窗口数量并按策略处理。
// 滚动/滑动：只对不存在的 (key,窗口) 计数；会话：只在不存在任何会话时预留 1 个/key。
// block 策略超限返回 ErrStateLimit（尚未做任何修改）；spill 策略驱逐最旧窗口。
func (s *shard) reserveCapacity(byKey map[string][]admittedEvent, keyOrder []string) error {
	eng := s.engine
	need := 0
	if eng.cfg.WindowType == WindowSession {
		for _, k := range keyOrder {
			if s.keys[k] == nil {
				need++
			}
		}
	} else {
		seen := map[string]struct{}{}
		for _, k := range keyOrder {
			ks := s.keys[k]
			for _, ae := range byKey[k] {
				for _, win := range eng.assigner.Assign(ae.e.Timestamp) {
					id := windowID(win, eng.assigner.Kind())
					token := k + "|" + id
					if _, dup := seen[token]; dup {
						continue
					}
					seen[token] = struct{}{}
					if ks == nil {
						need++
						continue
					}
					if _, ok := ks.windows[id]; !ok {
						need++
					}
				}
			}
		}
	}

	live := s.liveWindowCount()
	// 注：已存在窗口也可能因占位符惰性加载进入内存；上限按最终 live+new 估算。
	if live+need <= s.maxWin {
		return nil
	}
	overflow := live + need - s.maxWin

	switch eng.cfg.OnOverflow {
	case OverflowBlock:
		eng.log.Warn("state limit reached, applying backpressure",
			"shard", s.id, "live_windows", live, "need_new", need,
			"limit_per_shard", s.maxWin, "action", "block")
		return ErrStateLimit
	case OverflowSpill:
		return s.evictOldest(overflow)
	}
	return nil
}

// evictionCand 驱逐候选：优先驱逐最早结束的窗口（同结束按 windowID 排序，确定可解释）。
type evictionCand struct {
	key     string
	id      string
	endMs   int64
	startMs int64
}

// evictOldest 把至多 n 个最旧的、未标记 dirty（无未输出修订）的窗口落盘。
func (s *shard) evictOldest(n int) error {
	eng := s.engine
	var cands []evictionCand
	for k, ks := range s.keys {
		for id, st := range ks.windows {
			if st.spilledRef != "" || st.dirty {
				continue
			}
			cands = append(cands, evictionCand{
				key: k, id: id, endMs: st.w.End.UnixMilli(), startMs: st.w.Start.UnixMilli(),
			})
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].endMs != cands[j].endMs {
			return cands[i].endMs < cands[j].endMs
		}
		if cands[i].key != cands[j].key {
			return cands[i].key < cands[j].key
		}
		return cands[i].id < cands[j].id
	})
	evicted := 0
	for _, c := range cands {
		if evicted >= n {
			break
		}
		ks := s.keys[c.key]
		st := ks.windows[c.id]
		ref, err := eng.spill.spillWindow(c.key, c.id, st, eng.assigner.Kind())
		if err != nil {
			return err
		}
		placeholder := &windowState{
			w:          st.w,
			agg:        mustNewAgg(eng.cfg.Aggregate),
			seen:       map[string]struct{}{},
			fired:      st.fired,
			lastSeq:    st.lastSeq,
			lastValue:  st.lastValue,
			spilledRef: ref,
		}
		ks.windows[c.id] = placeholder
		evicted++
		eng.log.Info("window spilled to disk",
			"shard", s.id, "key", c.key, "window_id", c.id,
			"file", ref, "basis", "oldest-end-eviction")
	}
	if evicted < n {
		// 全部候选都驱逐后仍超限（例如活跃窗口本身就超过上限）：背压。
		eng.log.Warn("spill insufficient for state limit, applying backpressure",
			"shard", s.id, "evicted", evicted, "still_need", n-evicted)
		return ErrStateLimit
	}
	return nil
}
