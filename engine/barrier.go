package engine

import (
	"sort"
	"time"

	"github.com/lechandonga/stream-window-engine-go/window"
)

// fireAndPurge 在屏障水位线下扫描全部分片：
//   - 未 fired 且 End <= wm 的窗口 -> 首次输出（reason=window-fire）；
//   - 已 fired 且 dirty（迟到修订/会话合并）-> 修正输出（reason=late-revision /
//     session-revise）；
//   - wm >= End+AllowedLateness 的窗口状态被清除（固定窗口）；其 spill 文件删除；
//   - 会话窗口只在“已输出且超过 End+AllowedLateness”后清除（未触发的会话
//     即使在水位线下也继续保留等待迟到事件合并，直到 gap 空闲 + 允许迟到过期）。
//
// 返回合并后的草稿（已按 (endMs,key,windowID) 全局排序，Seq 在提交阶段统一分配）。
// 调用方持有 e.mu，并以分片为单位短暂获取 sh.mu。
// wm 是本批事件聚合后生效的水位线（首次触发用）；
// judgeWm 是本批事件准入时的水位线（迟到判定基准，不得被本批事件抬高）。
func (e *Engine) fireAndPurge(wm, judgeWm time.Time) []outDraft {
	var drafts []outDraft
	wmMs := wm.UnixMilli()
	judgeMs := judgeWm.UnixMilli()
	_ = judgeMs // 准入水位线仅用于触发扫描的语义说明（迟到判定在 apply 阶段完成）

	for _, sh := range e.shards {
		sh.mu.Lock()
		var keys []string
		for k := range sh.keys {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			ks := sh.keys[k]
			var ids []string
			for id := range ks.windows {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				st := ks.windows[id]
				endMs := st.w.End.UnixMilli()
				hardDeadline := st.w.End.Add(e.cfg.AllowedLateness).UnixMilli()

				// 落盘占位窗口的处理见下。
				if st.spilledRef != "" {
					// 超过硬期限：直接连同 spill 文件清理（后续迟到事件由
					// 无状态的 end+allowedLateness 判定识别为过期）。
					if wmMs >= hardDeadline && st.fired {
						if err := e.spill.remove(k, id); err != nil {
							e.log.Error("spill remove failed at barrier", "window_id", id, "err", err)
						}
						delete(ks.windows, id)
						continue
					}
					// 未 fired 但已到期，或已 fired 且可能需要处理迟到修订：
					// 惰性加载完整状态后走常规扫描逻辑。
					if endMs <= wmMs || st.fired {
						loaded, err := e.spill.loadWindow(id, st.spilledRef, e.assigner.Kind())
						if err != nil {
							e.log.Error("spill load failed at barrier", "window_id", id, "err", err)
							continue
						}
						e.spill.unregister(k, id, e.assigner.Kind())
						loaded.w = st.w
						loaded.fired = st.fired
						loaded.lastSeq = st.lastSeq
						loaded.lastValue = st.lastValue
						loaded.spilledRef = ""
						ks.windows[id] = loaded
						st = loaded
					} else {
						continue
					}
				}

				switch {
				case !st.fired && endMs <= wmMs:
					st.fired = true
					st.lastValue = st.agg.Value()
					st.dirty = false
					drafts = append(drafts, outDraft{
						key: k, windowID: id, kind: e.assigner.Kind(),
						correction: false, value: st.lastValue,
						reason: "window-fire", endMs: endMs,
					})
					e.log.Info("window fired",
						"key", k, "window_id", id, "end_ms", endMs,
						"watermark_ms", wmMs, "value", st.lastValue,
						"basis", "window-end<=watermark")
				case st.fired && st.dirty:
					st.lastValue = st.agg.Value()
					st.dirty = false
					reason := "late-revision"
					if e.cfg.WindowType == WindowSession {
						reason = "session-revise"
					}
					drafts = append(drafts, outDraft{
						key: k, windowID: id, kind: e.assigner.Kind(),
						correction: true, value: st.lastValue,
						reason: reason, endMs: endMs,
					})
					e.log.Info("window correction emitted",
						"key", k, "window_id", id, "watermark_ms", wmMs,
						"value", st.lastValue, "basis", "late-event-after-fire")
				}

				// 硬期限以本批生效后的水位线为准：迟到事件的“接受/拒绝”判定
				// 使用准入水位线（judgeMs），但状态清除发生在修订输出之后，
				// 用新水位线，避免“刚修订完即被清除”。
				if wmMs >= hardDeadline {
					if e.cfg.WindowType == WindowSession {
						if !st.fired {
							continue
						}
						if st.retracted {
							// 已撤销会话：其 ID 已被幸存者取代，不留墓碑。
							delete(ks.windows, id)
							continue
						}
					}
					e.log.Info("window state purged",
						"key", k, "window_id", id, "watermark_ms", wmMs,
						"deadline_ms", hardDeadline,
						"basis", "watermark>=end+allowedLateness")
					delete(ks.windows, id)
				}
			}
			if len(ks.windows) == 0 {
				delete(sh.keys, k)
			}
		}
		sh.mu.Unlock()
	}

	sortDrafts(drafts)
	return drafts
}

// sortDrafts 全局确定性排序：窗口结束时间 -> key -> windowID；
// 撤销（retract）记录与其幸存者修正属于同一屏障，retract 排在同窗口前面由
// reason 二级序保证。最终输出序列对任意事件到达顺序、任意分片数都一致。
func sortDrafts(d []outDraft) {
	sort.SliceStable(d, func(i, j int) bool {
		if d[i].endMs != d[j].endMs {
			return d[i].endMs < d[j].endMs
		}
		if d[i].key != d[j].key {
			return d[i].key < d[j].key
		}
		if d[i].windowID != d[j].windowID {
			return d[i].windowID < d[j].windowID
		}
		// 同一窗口：撤销在修正之前。
		return d[i].reason == "session-retract" && d[j].reason != "session-retract"
	})
}

var _ = window.Tumbling
