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
	if late {
		en.Dirty = true
		e.bumpStat(func(s *Stats) { s.LateHandled++ })
	}
	return e.mergeGroup(ctx, ev.Key, gap)
}

// sessionCluster 是一个确定的会话聚类。
type sessionCluster struct {
	win     window.Window
	members []state.Key // 按 Start、Seq 确定性排序
}

// clusterSessions 按 Gap 对同组会话做聚类（等价于 MergeSessions，
// 额外保留每个合并窗口的成员键）。
func clusterSessions(keys []state.Key, gap time.Duration) []sessionCluster {
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
		if len(clusters) == 0 || clusters[len(clusters)-1].win.End.UnixNano()+int64(gap) < k.Win.Start.UnixNano() {
			clusters = append(clusters, sessionCluster{win: k.Win, members: []state.Key{k}})
			continue
		}
		c := &clusters[len(clusters)-1]
		c.members = append(c.members, k)
		if k.Win.End.After(c.win.End) {
			c.win.End = k.Win.End
		}
		if k.Win.Seq < c.win.Seq {
			c.win.Seq = k.Win.Seq
		}
	}
	return clusters
}

// mergeGroup 对指定分组执行一次聚类并把多成员聚类落盘为单个条目。
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
	agg, _ := e.reg.Get(e.cfg.Aggregator)
	for _, c := range clusterSessions(groupKeys, gap) {
		if len(c.members) < 2 {
			continue
		}
		merged := agg.Init()
		var revision int64
		emittedAny, closedAny, dirtyAny := false, false, false
		for _, mk := range c.members {
			en, ok, err := e.store.Get(ctx, mk)
			if err != nil || !ok {
				return err
			}
			for id := range en.Acc.Seen {
				merged.Seen[id] = struct{}{}
			}
			// 成员间 Seen 互不相交（事件只进一个初始会话），聚合量直接相加。
			merged.Value += en.Acc.Value
			merged.Count += en.Acc.Count
			revision = maxInt64(revision, en.Revision)
			emittedAny = emittedAny || en.Emitted
			closedAny = closedAny || en.Closed
			dirtyAny = dirtyAny || en.Dirty
		}
		target := state.Key{Group: group, Win: c.win}
		if err := e.store.Put(target, &state.Entry{
			Acc: merged, Revision: revision, Emitted: emittedAny,
			Closed: closedAny, Dirty: dirtyAny,
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

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
