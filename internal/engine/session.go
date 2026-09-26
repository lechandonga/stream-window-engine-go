package engine

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// applySession 把事件写入会话窗口，并与同组中可合并的活动会话合并。
// 合并规则只取决于时间区间与 Gap，与到达顺序无关；
// 一个事件初始只属于一个会话，因此不同成员的去重集合互不相交。
func (e *Engine) applySession(ctx context.Context, k state.Key, ev event.Event, late bool) error {
	e.mu.Lock()
	e.sessionSeq++
	mySeq := e.sessionSeq
	aggName := e.cfg.Aggregator
	gap := e.cfg.Window.Gap
	ml, ok := e.mergeLocks[ev.Key]
	if !ok {
		ml = &sync.Mutex{}
		e.mergeLocks[ev.Key] = ml
	}
	e.mu.Unlock()

	// 同组会话的"写初始窗口 + 聚类合并"必须互斥，
	// 避免并发 worker 合并出不同中间结果。
	ml.Lock()
	defer ml.Unlock()

	// 合并前去重：迟到重复投递的事件可能属于"已被并入其他会话"的
	// 旧窗口，其初始窗口键已不存在。若只在新建初始条目内去重，该事件
	// 会先被计数、再在合并时去重，平白把存活会话标记 dirty 产生多余
	// CORRECTION。这里先扫描同组全部现存会话的去重集合，命中即拒绝。
	if ev.ID != "" {
		dup, err := e.isGroupSeen(ctx, ev.Key, ev.ID)
		if err != nil {
			return err
		}
		if dup {
			e.logDuplicate(ev, k.Win, e.wm.Get())
			e.bumpStat(func(s *Stats) { s.Duplicates++ })
			return nil
		}
	}

	k.Win.Seq = mySeq
	en, err := e.store.GetOrCreate(ctx, k, aggName)
	if err != nil {
		return err
	}
	if !en.Acc.Add(ev) {
		e.logDuplicate(ev, k.Win, e.wm.Get())
		e.bumpStat(func(s *Stats) { s.Duplicates++ })
		return nil
	}
	if ev.Ts.After(en.MaxEvent) {
		en.MaxEvent = ev.Ts
	}
	if late {
		en.Dirty = true
		e.bumpStat(func(s *Stats) { s.LateHandled++ })
	}
	return e.mergeGroup(ctx, ev.Key, gap)
}

// sessionCluster 是一个确定的会话聚类。
type sessionCluster struct {
	win      window.Window
	maxEvent time.Time   // 成员中最晚的事件时间（会话末端由它 +Gap 决定）
	members  []state.Key // 按 Start、Seq 确定性排序
}

// clusterSessions 按 Gap 对同组会话条目做聚类。连接判定与窗口 End
// 都基于"会话内最近一次事件时间"（成员条目的 MaxEvent；缺省回退到
// 单事件初始窗口的 Start）：
//
//	prev.maxEvent + gap >= cur.Start -> 同一会话（间隔恰好等于
//	配置上限也算同一个）；严格大于则分裂为两个独立会话。
//
// 不能用初始窗口 End（= ts+gap）作连接基准：那会让两个间隔略大于
// gap 的会话被错误桥接；也不能在乱序下用中间窗口 End 的最大值，
// 否则先到晚事件、后到早事件会得到偏小的 End。
func clusterSessions(loaded map[state.Key]*state.Entry, keys []state.Key, gap time.Duration) []sessionCluster {
	maxEventOf := func(k state.Key) time.Time {
		if en, ok := loaded[k]; ok && !en.MaxEvent.IsZero() {
			return en.MaxEvent
		}
		return k.Win.Start
	}
	sorted := append([]state.Key(nil), keys...)
	sort.SliceStable(sorted, func(i, j int) bool {
		a, b := sorted[i].Win, sorted[j].Win
		if !a.Start.Equal(b.Start) {
			return a.Start.Before(b.Start)
		}
		if !a.End.Equal(b.End) {
			return a.End.Before(b.End)
		}
		return a.Seq < b.Seq
	})
	var clusters []sessionCluster
	for _, k := range sorted {
		ev := maxEventOf(k)
		if len(clusters) == 0 || clusters[len(clusters)-1].maxEvent.UnixNano()+int64(gap) < k.Win.Start.UnixNano() {
			win := k.Win
			win.End = ev.Add(gap)
			clusters = append(clusters, sessionCluster{
				win:      win,
				maxEvent: ev,
				members:  []state.Key{k},
			})
			continue
		}
		c := &clusters[len(clusters)-1]
		c.members = append(c.members, k)
		if ev.After(c.maxEvent) {
			c.maxEvent = ev
		}
		// 合并后窗口 End = 最晚事件 + Gap，Seq 取最小（身份稳定）。
		c.win.End = c.maxEvent.Add(gap)
		if k.Win.Seq < c.win.Seq {
			c.win.Seq = k.Win.Seq
		}
	}
	return clusters
}

// mergeGroup 对指定分组执行一次聚类并把多成员聚类落盘为单个条目。
//
// 撤销语义：聚类的存活谱系（ResultID）取最早 Start 的成员——该 Start
// 在会话延长/级联合并时都不变，因此同一会话谱系的 INSERT、
// CORRECTION 由同一 ResultID 串起；被并入的其他成员若已发布，
// 其最后一次发布快照作为 Retraction 排到存活条目的 Pending 队列，
// 在屏障点随存活结果的替代输出一起、按 ResultID 确定性顺序发出。
//
// 幂等与确定：合并纯由 (成员窗口, Gap) 决定，与到达顺序无关；
// 待发撤销按 ResultID 去重，级联合并（成员本身已携带 Pending）时
// 旧撤销原样转移到新存活条目，绝不丢失也不重复。
func (e *Engine) mergeGroup(ctx context.Context, group string, gap time.Duration) error {
	keys, err := e.store.ListKeys(ctx)
	if err != nil {
		return err
	}
	var groupKeys []state.Key
	for _, kk := range keys {
		if kk.Group == group && kk.Win.Kind == window.Session {
			groupKeys = append(groupKeys, kk)
		}
	}
	if len(groupKeys) < 2 {
		return nil
	}
	// 预加载全部成员条目，供聚类基于 MaxEvent 计算（顺序无关）。
	loaded := make(map[state.Key]*state.Entry, len(groupKeys))
	for _, kk := range groupKeys {
		en, ok, err := e.store.Get(ctx, kk)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		loaded[kk] = en
	}
	agg, _ := e.reg.Get(e.cfg.Aggregator)
	for _, c := range clusterSessions(loaded, groupKeys, gap) {
		if len(c.members) < 2 {
			continue
		}
		// members 已按 (Start,End,Seq) 排序，第一个即存活谱系。
		survivor := c.members[0]
		survivorID := resultIDOfKey(survivor)
		target := state.Key{Group: group, Win: c.win}

		merged := agg.Init()
		emittedAny, dirtyAny := false, false
		var maxEvent time.Time
		var pending []state.Retraction
		pendingSeen := map[string]struct{}{}
		enqueue := func(r state.Retraction) {
			if _, ok := pendingSeen[r.ResultID]; ok {
				return
			}
			pendingSeen[r.ResultID] = struct{}{}
			pending = append(pending, r)
		}

		for _, mk := range c.members {
			en := loaded[mk]
			// 跨成员按事件 ID 去重合并：迟到的重复投递只计一次。
			merged.MergeInto(en.Acc)
			if en.MaxEvent.After(maxEvent) {
				maxEvent = en.MaxEvent
			}
			// 成员此前排队的撤销（级联合并）原样转移、去重。
			for _, r := range en.Pending {
				enqueue(r)
			}
			emittedAny = emittedAny || en.Emitted
			dirtyAny = dirtyAny || en.Dirty
			// 存活谱系本身不撤销；其余已发布成员需要撤销。
			// 已发布会话只能被迟到数据连接合并（wm 早已越过其 End），
			// 故撤销消息恒为迟到修正类。
			if en.Emitted && resultIDOfKey(mk) != survivorID {
				enqueue(state.Retraction{
					ResultID: resultIDOfKey(mk),
					Revision: en.Revision,
					Win:      mk.Win,
					Value:    en.Acc.Value,
					Count:    en.Acc.Count,
					Late:     true,
				})
			}
		}
		// 撤销按 ResultID 排序，保证发出顺序与并发/到达顺序无关。
		sort.Slice(pending, func(i, j int) bool { return pending[i].ResultID < pending[j].ResultID })

		// 存活条目的修订号沿用其自身谱系：已发布则下一条为 +1 的
		// CORRECTION；未发布时保持 0，由触发器作为首版 INSERT 发出。
		survivorRevision := loaded[survivor].Revision
		survivorEmitted := loaded[survivor].Emitted
		// 吸收了已发布成员（产生撤销）或自身有迟到更新：存活结果
		// 必须重发（INSERT 或 CORRECTION），与撤销在同一屏障配对。
		if survivorEmitted || emittedAny || dirtyAny {
			dirtyAny = true
		}
		if err := e.store.Put(target, &state.Entry{
			Acc:      merged,
			Revision: survivorRevision,
			Emitted:  survivorEmitted,
			Closed:   false, // 宽限期判定只由触发器依据 End+allowedLateness 重做
			Dirty:    dirtyAny,
			MaxEvent: maxEvent,
			Pending:  pending,
		}); err != nil {
			return err
		}
		for _, mk := range c.members {
			if mk == target {
				continue
			}
			if err := e.store.Delete(mk); err != nil {
				return err
			}
		}
	}
	return nil
}

// isGroupSeen 报告事件 ID 是否已存在于该分组任一会话的去重集合。
// 调用方已持有该分组的合并锁，故扫描期间同组窗口结构稳定。
func (e *Engine) isGroupSeen(ctx context.Context, group, id string) (bool, error) {
	keys, err := e.store.ListKeys(ctx)
	if err != nil {
		return false, err
	}
	for _, kk := range keys {
		if kk.Group != group || kk.Win.Kind != window.Session {
			continue
		}
		en, ok, err := e.store.Get(ctx, kk)
		if err != nil {
			return false, err
		}
		if !ok || en.Acc == nil {
			continue
		}
		if _, seen := en.Acc.Seen[id]; seen {
			return true, nil
		}
	}
	return false, nil
}
