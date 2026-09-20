package engine

import (
	"sort"
	"time"

	"github.com/lechandonga/stream-window-engine-go/window"
)

// applySessionKey 在单个 key 上顺序应用事件到动态会话窗口。
//
// 语义（确定、可解释）：
//   - 新会话候选 [ts, ts+gap)；收集所有与之重叠（start < other.end 且 end > other.start）
//     的现存会话（包含事件时间落入的会话），整体传递性地合并为一个连通簇。
//   - 幸存者恒为簇中开始时间最早的窗口（同开始取 windowID 最小）——保证已输出窗口
//     的 ID 不随合并变化；其余被合并窗口若已输出则发撤销（correction, value=0,
//     reason=session-retract），使下游能删除过期结果。
//   - 合并采用差量：只把幸存者未见事件 ID 的值相加，重复投递不重复计数。
//   - 迟到事件（事件时间 < 水位线）按 LateEventPolicy 处理；revise 下只要
//     水位线 < 相关最晚边界 end+AllowedLateness 即可重开/延长已关闭会话。
func (s *shard) applySessionKey(k string, ks *keyState, events []admittedEvent, wm time.Time, extra *batchExtra, drafts []outDraft) ([]outDraft, error) {
	eng := s.engine
	gap := eng.assigner.(*window.SessionAssigner).Gap()
	wmMs := wm.UnixMilli()

	for _, ae := range events {
		ts := ae.e.Timestamp
		tsMs := ts.UnixMilli()
		newEnd := ts.Add(gap)

		// 1) 收集与候选会话 [ts,newEnd) 重叠的全部现存会话。
		type cand struct {
			id string
			st *windowState
		}
		var cluster []cand
		for id, st := range ks.windows {
			// 左闭右开：端点相等不算重叠。
			if ts.Before(st.w.End) && newEnd.After(st.w.Start) {
				cluster = append(cluster, cand{id: id, st: st})
			}
		}

		// 2) 迟到判定（以簇中最晚 end 或新会话 end 为基准判定 allowed lateness）。
		if ae.late {
			limitMs := newEnd.UnixMilli() + eng.cfg.AllowedLateness.Milliseconds()
			for _, c := range cluster {
				if v := c.st.w.End.Add(eng.cfg.AllowedLateness).UnixMilli(); v > limitMs {
					limitMs = v
				}
			}
			expired := wmMs >= limitMs
			rec := LateEventRecord{
				EventID: ae.e.ID, Key: k, Watermark: wmMs, EventTime: tsMs,
				WindowID: "", Policy: string(eng.cfg.LateEventPolicy),
			}
			switch eng.cfg.LateEventPolicy {
			case LateDrop:
				rec.Action = "late-drop"
				if expired {
					rec.Action = "late-drop-expired"
				}
				extra.recordLate(rec)
				eng.log.Warn("late session event dropped", "event_id", ae.e.ID, "key", k,
					"watermark_ms", wmMs, "event_time_ms", tsMs, "expired", expired, "action", rec.Action)
				continue
			case LateRedirect:
				rec.Action = "late-redirect"
				extra.recordLate(rec)
				extra.recordSide(SideOutputRecord{EventID: ae.e.ID, Key: k, EventTime: tsMs, Value: ae.e.Value, Watermark: wmMs})
				eng.log.Warn("late session event redirected", "event_id", ae.e.ID, "key", k, "action", rec.Action)
				continue
			case LateRevise:
				if expired {
					rec.Action = "late-drop-expired"
					extra.recordLate(rec)
					eng.log.Warn("late session event beyond allowed lateness",
						"event_id", ae.e.ID, "key", k, "watermark_ms", wmMs,
						"event_time_ms", tsMs, "action", rec.Action,
						"basis", "watermark>=cluster-end+allowedLateness")
					continue
				}
				rec.Action = "late-revise"
				extra.recordLate(rec)
				eng.log.Info("late session event accepted for revision",
					"event_id", ae.e.ID, "key", k, "watermark_ms", wmMs, "event_time_ms", tsMs)
			}
		}

		// 3) 确定幸存者：开始最早（ID 最小），簇为空则新建。
		var survivor *windowState
		var survivorID string
		if len(cluster) == 0 {
			fresh, err := newWindowState(window.Window{Start: ts, End: newEnd}, eng.cfg.Aggregate)
			if err != nil {
				return nil, err
			}
			survivorID = windowID(fresh.w, window.Session)
			ks.windows[survivorID] = fresh
			survivor = fresh
			eng.log.Info("new session created", "event_id", ae.e.ID, "key", k,
				"window_id", survivorID, "start_ms", fresh.w.Start.UnixMilli(),
				"end_ms", fresh.w.End.UnixMilli())
		} else {
			sort.Slice(cluster, func(i, j int) bool {
				if !cluster[i].st.w.Start.Equal(cluster[j].st.w.Start) {
					return cluster[i].st.w.Start.Before(cluster[j].st.w.Start)
				}
				return cluster[i].id < cluster[j].id
			})
			survivorID = cluster[0].id
			survivor = cluster[0].st
		}

		// 4) 事件计入幸存者（去重），并按需延长尾部。
		if survivor.addOnce(ae.e) {
			if ae.e.ID != "" {
				survivor.values[ae.e.ID] = ae.e.Value
			}
			eng.log.Info("session event assigned", "event_id", ae.e.ID, "key", k,
				"window_id", survivorID, "event_time_ms", tsMs, "basis", "session-cluster")
		}
		if newEnd.After(survivor.w.End) {
			survivor.w.End = newEnd
		}

		// 5) 合并簇中其余窗口：边界取并集，聚合按未见事件 ID 做差量。
		sort.Slice(cluster, func(i, j int) bool { return cluster[i].id < cluster[j].id })
		for _, c := range cluster {
			if c.st == survivor {
				continue
			}
			victimID, victim := c.id, c.st
			if victim.w.End.After(survivor.w.End) {
				survivor.w.End = victim.w.End
			}
			// 差量：victim 中 survivor 未见过的事件值。
			delta, err := newWindowState(victim.w, eng.cfg.Aggregate)
			if err != nil {
				return nil, err
			}
			for eid := range victim.seen {
				if _, ok := survivor.seen[eid]; ok {
					continue
				}
				survivor.seen[eid] = struct{}{}
				survivor.values[eid] = victim.values[eid]
				delta.agg.Add(victim.values[eid])
			}
			survivor.agg.Merge(delta.agg)

			if victim.fired && !victim.retracted {
				drafts = append(drafts, outDraft{
					key: k, windowID: victimID, kind: window.Session,
					correction: true, value: 0, reason: "session-retract",
					endMs: victim.w.End.UnixMilli(),
				})
				victim.retracted = true
			}
			if victim.spilledRef != "" {
				eng.spill.markPendingDelete(victim.spilledRef)
			}
			delete(ks.windows, victimID)
			if victim.spilledRef == "" {
			}
			eng.log.Info("session windows merged", "key", k,
				"survivor_id", survivorID, "removed_id", victimID,
				"new_start_ms", survivor.w.Start.UnixMilli(), "new_end_ms", survivor.w.End.UnixMilli())
		}

		if ae.late && eng.cfg.LateEventPolicy == LateRevise && survivor.fired {
			survivor.dirty = true
		}
	}
	return drafts, nil
}
