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
// 会话窗口标识只由 (分组, Start, End) 决定：Window.Seq 恒为 0，
// 因此同一批事件以任意顺序到达、任意并发度、任意重放都得到相同标识。
func (e *Engine) applySession(ctx context.Context, k state.Key, ev event.Event, late bool) error {
	e.mu.Lock()
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

	// 重复投递/重启重放去重：同组任一会话已见过该事件 ID 则跳过。
	// 必须在创建新初始会话之前检查——新条目的去重集合为空，
	// 直接 Add 会让重复事件被二次计数。
	if ev.ID != "" {
		dup, err := e.seenInGroup(ctx, ev.Key, ev.ID)
		if err != nil {
			return err
		}
		if dup {
			e.logDuplicate(ev, k.Win, e.wm.Get())
			e.bumpStat(func(s *Stats) { s.Duplicates++ })
			return nil
		}
	}

	k.Win.Seq = 0
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

// seenInGroup 报告事件 ID 是否已出现在该分组任一会话窗口的去重集合中。
// 调用方需持有该分组的合并锁。
func (e *Engine) seenInGroup(ctx context.Context, group, id string) (bool, error) {
	keys, err := e.store.ListKeys(ctx)
	if err != nil {
		return false, err
	}
	for _, kk := range keys {
		if kk.Group != group || kk.Win.Kind != window.Session {
			continue
		}
		en, ok, err := e.store.Get(ctx, kk)
		if err != nil || !ok {
			return false, err
		}
		if en.Acc != nil {
			if _, seen := en.Acc.Seen[id]; seen {
				return true, nil
			}
		}
	}
	return false, nil
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
		return a.Seq < b.Seq // 恒为 0，仅保持稳定排序
	})
	var clusters []sessionCluster
	for _, k := range sorted {
		// 初始会话为 [ts, ts+gap)：prev.End >= cur.Start 恰好等价于
		// 相邻事件时间差 <= gap。等于 gap 合并，超过 gap（含 1ns）独立。
		if len(clusters) == 0 || clusters[len(clusters)-1].win.End.UnixNano() < k.Win.Start.UnixNano() {
			clusters = append(clusters, sessionCluster{win: k.Win, members: []state.Key{k}})
			continue
		}
		c := &clusters[len(clusters)-1]
		c.members = append(c.members, k)
		if k.Win.End.After(c.win.End) {
			c.win.End = k.Win.End
		}
		c.win.Seq = 0 // 会话标识与到达顺序无关
	}
	return clusters
}

// mergeGroup 对指定分组执行一次聚类并把多成员聚类落盘为单个条目。
//
// 撤销语义（本轮新增）：
//   - 若被合并的成员已向外发出过结果，且其窗口标识不等于合并目标，
//     该已发布结果不再成立：登记一条待撤销（含被撤销时的修订号与
//     聚合值、替代窗口），由屏障触发阶段统一发射 RETRACT；
//   - 合并目标是新标识：作为全新结果，发射状态重置（下次触发为
//     INSERT rev1），旧标识的结果通过撤销通道告知下游；
//   - 合并目标与某成员标识相同（该成员区间覆盖其余成员）：内容并入
//     既有标识，保留其 Revision/Emitted，标记 Dirty——若已发出过，
//     下次触发为 CORRECTION，无需撤销。
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
		type member struct {
			key state.Key
			en  *state.Entry
		}
		members := make([]member, 0, len(c.members))
		merged := agg.Init()
		closedAny := false
		for _, mk := range c.members {
			en, ok, err := e.store.Get(ctx, mk)
			if err != nil || !ok {
				return err
			}
			members = append(members, member{key: mk, en: en})
			for id := range en.Acc.Seen {
				merged.Seen[id] = struct{}{}
			}
			// 成员间 Seen 互不相交（事件只进一个初始会话），聚合量直接相加。
			merged.Value += en.Acc.Value
			merged.Count += en.Acc.Count
			closedAny = closedAny || en.Closed
		}
		target := state.Key{Group: group, Win: c.win}
		out := &state.Entry{Acc: merged, Closed: closedAny}
		for _, m := range members {
			if m.key == target {
				// 标识保留：沿用既有发射状态；内容必然变化（有新成员并入）。
				out.Emitted = m.en.Emitted
				out.Revision = m.en.Revision
				out.Dirty = true
				continue
			}
			if m.en.Emitted {
				e.noteRetract(pendingRetract{
					key:        m.key,
					revision:   m.en.Revision,
					value:      m.en.Acc.Value,
					count:      m.en.Acc.Count,
					replacedBy: c.win,
				})
			}
		}
		if err := e.store.Put(target, out); err != nil {
			return err
		}
		for _, m := range members {
			if m.key == target {
				continue
			}
			if err := e.store.Delete(m.key); err != nil {
				return err
			}
		}
	}
	return nil
}
