package engine

import (
	"time"

	"github.com/lechandonga/stream-window-engine-go/aggregate"
	"github.com/lechandonga/stream-window-engine-go/event"
	"github.com/lechandonga/stream-window-engine-go/window"
)

const aggregateKindSum = aggregate.Sum

// admittedEvent 是批次准入后的事件及其迟到标记（对比批次开始时的水位线）。
type admittedEvent struct {
	e    event.Event
	late bool
}

// outDraft 是屏障处待输出的草稿（Seq 在统一排序后分配，保证确定性）。
type outDraft struct {
	key        string
	windowID   string
	kind       window.Kind
	correction bool
	value      int64
	reason     string
	endMs      int64
}

// batchExtra 收集分片在并行应用期间产生的记录（引擎级切片只能在屏障串行点合并）。
type batchExtra struct {
	late []LateEventRecord
	side []SideOutputRecord
}

func (x *batchExtra) recordLate(r LateEventRecord)  { x.late = append(x.late, r) }
func (x *batchExtra) recordSide(r SideOutputRecord) { x.side = append(x.side, r) }

// applyBatch 在单个分片内顺序应用一个批次的事件（同一 key 的顺序与输入一致）。
// wm 是批次准入时（推进前）的水位线。
// 调用方持有 e.mu（保证批次间串行），本函数只获取 sh.mu。
func (s *shard) applyBatch(batch []admittedEvent, wm time.Time) ([]outDraft, *batchExtra, error) {
	extra := &batchExtra{}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.engine.log.Info("shard apply batch",
		"shard", s.id, "events", len(batch),
		"watermark_ms", msTime(wm))

	// 按 key 分组并保持批次内顺序。
	byKey := map[string][]admittedEvent{}
	keyOrder := make([]string, 0)
	for _, ae := range batch {
		if _, ok := byKey[ae.e.Key]; !ok {
			keyOrder = append(keyOrder, ae.e.Key)
		}
		byKey[ae.e.Key] = append(byKey[ae.e.Key], ae)
	}

	eng := s.engine

	if eng.cfg.WindowType == WindowSession {
		for _, k := range keyOrder {
			ks := s.keys[k]
			if ks == nil {
				continue
			}
			if err := s.materializeKey(k); err != nil {
				return nil, nil, err
			}
		}
	}

	// 容量预判：估算本批次需要新建的窗口数，超限先按策略处理（保证失败不留半更新）。
	if eng.cfg.MaxWindows > 0 {
		if err := s.reserveCapacity(byKey, keyOrder); err != nil {
			return nil, nil, err
		}
	}

	var drafts []outDraft
	for _, k := range keyOrder {
		ks := s.keys[k]
		if ks == nil {
			ks = newKeyState()
			s.keys[k] = ks
		}
		var err error
		if eng.cfg.WindowType == WindowSession {
			drafts, err = s.applySessionKey(k, ks, byKey[k], wm, extra, drafts)
		} else {
			drafts, err = s.applyFixedKey(k, ks, byKey[k], wm, extra, drafts)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return drafts, extra, nil
}

// applyFixedKey 处理滚动/滑动窗口（容量已在批次开始时预留）。
func (s *shard) applyFixedKey(k string, ks *keyState, events []admittedEvent, wm time.Time, extra *batchExtra, drafts []outDraft) ([]outDraft, error) {
	eng := s.engine
	for _, ae := range events {
		for _, win := range eng.assigner.Assign(ae.e.Timestamp) {
			id := windowID(win, eng.assigner.Kind())
			// 无状态过期判定：窗口状态不存在时，只要 end+allowedLateness
			// 已不晚于准入水位线，事件不可能再属于任何活动窗口，明确记录为
			// 过期（不创建状态、不静默丢弃，且无需无界增长的墓碑结构）。
			if win.End.Add(eng.cfg.AllowedLateness).Compare(wm) <= 0 {
				action := "late-drop-expired"
				if eng.cfg.LateEventPolicy == LateRedirect {
					action = "late-redirect"
					extra.recordSide(SideOutputRecord{
						EventID:   ae.e.ID,
						Key:       k,
						EventTime: ae.e.Timestamp.UnixMilli(),
						Value:     ae.e.Value,
						Watermark: wm.UnixMilli(),
					})
				}
				extra.recordLate(LateEventRecord{
					EventID:   ae.e.ID,
					Key:       k,
					Watermark: wm.UnixMilli(),
					EventTime: ae.e.Timestamp.UnixMilli(),
					WindowID:  id,
					Policy:    string(eng.cfg.LateEventPolicy),
					Action:    action,
				})
				eng.log.Warn("event for expired window rejected",
					"event_id", ae.e.ID, "key", k, "window_id", id,
					"end_ms", win.End.UnixMilli(), "action", action,
					"basis", "end+allowedLateness<=watermark")
				continue
			}
			st, ok := ks.windows[id]
			if !ok {
				fresh, err := newWindowState(win, eng.cfg.Aggregate)
				if err != nil {
					return nil, err
				}
				ks.windows[id] = fresh
				st = fresh
			} else if st.spilledRef != "" {
				loaded, err := s.getOrLoad(k, ks, id, win, eng.assigner.Kind())
				if err != nil {
					return nil, err
				}
				st = loaded
			}
			s.classifyAndAdd(k, id, ae, st, wm, extra)
		}
	}
	return drafts, nil
}

// classifyAndAdd 按水位线与策略处置单个（事件,窗口）归属，返回动作描述（用于日志）。
func (s *shard) classifyAndAdd(key, id string, ae admittedEvent, st *windowState, wm time.Time, extra *batchExtra) string {
	eng := s.engine
	expiredMs := st.w.End.Add(eng.cfg.AllowedLateness).UnixMilli()
	wmMs := wm.UnixMilli()

	// 情况 1：窗口尚未首次输出——无论事件是否早于水位线，都计入首次输出
	//（窗口在本批次屏障才关闭；判定依据：fired=false）。
	if !st.fired {
		if st.addOnce(ae.e) {
			eng.log.Info("event assigned to open window",
				"event_id", ae.e.ID, "key", key, "window_id", id,
				"event_time_ms", ae.e.Timestamp.UnixMilli(), "basis", "window-not-fired")
		}
		return "assigned-first-fire"
	}

	// 情况 2：窗口已输出。事件时间早于水位线即为迟到。
	if !ae.late {
		st.addOnce(ae.e)
		return "assigned"
	}

	expired := wmMs >= expiredMs
	rec := LateEventRecord{
		EventID:   ae.e.ID,
		Key:       key,
		Watermark: wmMs,
		EventTime: ae.e.Timestamp.UnixMilli(),
		WindowID:  id,
		Policy:    string(eng.cfg.LateEventPolicy),
	}
	switch eng.cfg.LateEventPolicy {
	case LateDrop:
		rec.Action = "late-drop"
		if expired {
			rec.Action = "late-drop-expired"
		}
		extra.recordLate(rec)
		eng.log.Warn("late event dropped",
			"event_id", ae.e.ID, "key", key, "window_id", id,
			"watermark_ms", wmMs, "event_time_ms", rec.EventTime,
			"expired", expired, "action", rec.Action,
			"basis", "event-time-before-watermark")
		return rec.Action
	case LateRedirect:
		rec.Action = "late-redirect"
		extra.recordLate(rec)
		eng.recordSide(SideOutputRecord{
			EventID: ae.e.ID, Key: key, EventTime: rec.EventTime,
			Value: ae.e.Value, Watermark: wmMs,
		})
		eng.log.Warn("late event redirected to side output",
			"event_id", ae.e.ID, "key", key, "window_id", id,
			"watermark_ms", wmMs, "event_time_ms", rec.EventTime, "action", rec.Action)
		return rec.Action
	case LateRevise:
		if expired {
			rec.Action = "late-drop-expired"
			extra.recordLate(rec)
			eng.log.Warn("late event beyond allowed lateness dropped",
				"event_id", ae.e.ID, "key", key, "window_id", id,
				"watermark_ms", wmMs, "event_time_ms", rec.EventTime,
				"allowed_lateness_ms", eng.cfg.AllowedLateness.Milliseconds(),
				"action", rec.Action, "basis", "watermark>=end+allowedLateness")
			return rec.Action
		}
		if st.addOnce(ae.e) {
			st.dirty = true
			rec.Action = "late-revise"
			extra.recordLate(rec)
			eng.log.Info("late event accepted for revision",
				"event_id", ae.e.ID, "key", key, "window_id", id,
				"watermark_ms", wmMs, "event_time_ms", rec.EventTime,
				"new_value", st.agg.Value(), "action", rec.Action)
		}
		return "late-revise"
	}
	return ""
}

// addOnce 按事件 ID 去重后计入聚合；重复投递返回 false（不重复计数）。
// 事件 ID 在准入时已保证非空（缺失 ID 会得到确定性的合成 ID）。
func (st *windowState) addOnce(e event.Event) bool {
	if _, ok := st.seen[e.ID]; ok {
		return false
	}
	contrib := int64(1)
	if st.agg.Kind() == aggregateKindSum {
		contrib = e.Value
	}
	st.agg.Add(e.Value)
	st.seen[e.ID] = struct{}{}
	if st.values == nil {
		st.values = map[string]int64{}
	}
	if st.contribs == nil {
		st.contribs = map[string]int64{}
	}
	st.values[e.ID] = e.Value
	st.contribs[e.ID] = contrib
	return true
}
