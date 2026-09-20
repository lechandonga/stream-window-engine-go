package engine

import (
	"fmt"
	"sync"
	"time"

	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/event"
	"github.com/lechandonga/stream-window-engine-go/sink"
)

// ProcessEvent 注入单个事件（等价于单事件批次）。
// 返回 false 且 err==ErrInvalidEventTime 表示事件按时间策略被拒绝（已记录）；
// 返回 ErrStateLimit 表示背压，调用方可稍后重试（状态未发生任何变更）。
func (e *Engine) ProcessEvent(e0 event.Event) (bool, error) {
	n, err := e.ProcessBatch([]event.Event{e0})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ProcessBatch 处理一批事件。
//
// 语义保证：
//   - 批次在全局串行点上准入：校验时间、标记迟到、推进水位线；
//   - 随后各分片并行应用（仅状态计算），屏障阶段再次全局串行：
//     触发到期窗口、统一排序分配 Seq、原子检查点、按序投递；
//   - 因而跨 key 并发的可观察输出与按输入顺序的串行处理完全一致。
//
// 返回被接受进入聚合的事件数（被拒绝/丢弃的计入各类记录）。
func (e *Engine) ProcessBatch(batch []event.Event) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, fmt.Errorf("engine closed")
	}

	wmBefore := e.wm.Current()
	accepted := make([]admittedEvent, 0, len(batch))
	acceptedN := 0
	for _, ev := range batch {
		// 缺失 ID：确定性合成 ID（基于到达序号与内容），保证去重机制始终可用。
		if ev.ID == "" {
			ev.ID = fmt.Sprintf("auto-%d-%d-%s-%d", e.eventCount, ev.Timestamp.UnixNano(), ev.Key, ev.Value)
		}
		validated, reason, rejected := e.validator.Validate(ev)
		if reason != event.ReasonOK {
			rec := InvalidEventRecord{
				EventID: ev.ID, Key: ev.Key, Reason: reason.String(),
				Policy: e.policyString(),
			}
			e.recordInvalid(rec)
			if rejected {
				e.log.Warn("event rejected by timestamp policy",
					"event_id", ev.ID, "key", ev.Key, "reason", reason.String(),
					"policy", rec.Policy,
					"basis", "missing-or-abnormal-event-time")
				continue
			}
			e.log.Warn("event time reassigned by policy",
				"event_id", ev.ID, "key", ev.Key, "reason", reason.String(),
				"policy", rec.Policy, "assigned_time_ms", validated.Timestamp.UnixMilli())
		}
		if rejected {
			continue
		}

		late := e.wm.IsInitialized() && validated.Timestamp.Before(wmBefore)
		accepted = append(accepted, admittedEvent{e: validated, late: late})
		acceptedN++
		e.eventCount++
	}

	if len(accepted) == 0 {
		return 0, nil
	}

	// 推进水位线（只升不降），用本批最大事件时间。
	maxTS := accepted[0].e.Timestamp
	for _, ae := range accepted[1:] {
		if ae.e.Timestamp.After(maxTS) {
			maxTS = ae.e.Timestamp
		}
	}
	wmNew := e.wm.Observe(maxTS)
	if wmNew.Equal(wmBefore) {
		// 没有推进也要进入屏障（迟到修订可能需要输出）。
	}

	// 分片并行应用状态计算。
	perShard := make([][]admittedEvent, len(e.shards))
	for _, ae := range accepted {
		si := shardForKey(ae.e.Key, len(e.shards))
		perShard[si] = append(perShard[si], ae)
	}
	var (
		wg        sync.WaitGroup
		applyErr  error
		errOnce   sync.Once
		resultsMu sync.Mutex
		allDrafts []outDraft
		allExtras []*batchExtra
	)
	wg.Add(len(e.shards))
	for i, sh := range e.shards {
		go func(idx int, sh *shard, items []admittedEvent) {
			defer wg.Done()
			if len(items) == 0 {
				return
			}
			d, ex, err := sh.applyBatch(items, wmBefore)
			errOnce.Do(func() { applyErr = err })
			if err == nil {
				resultsMu.Lock()
				allDrafts = append(allDrafts, d...)
				allExtras = append(allExtras, ex)
				resultsMu.Unlock()
			}
		}(i, sh, perShard[i])
	}
	wg.Wait()
	if applyErr != nil {
		// 背压（ErrStateLimit）发生在 reserveCapacity 阶段、任何状态变更之前，
		// 不留下半更新；落盘失败视为致命错误（failed 态），需从最近检查点恢复。
		e.failed = true
		return 0, applyErr
	}
	// 在屏障串行点（持有 e.mu）合并各分片的迟到/侧输出记录。
	for _, ex := range allExtras {
		e.latedEvents = append(e.latedEvents, ex.late...)
		e.sideOutputs = append(e.sideOutputs, ex.side...)
	}

	// 屏障：触发/修订一律以“本批准入时的水位线”为判定基准（wmBefore），
	// 保证迟到事件在任何水位线推进之前完成判定与聚合；本批产生的新水位线
	// 将在事件聚合之后（屏障末）生效，用于后续批次。
	fired := e.fireAndPurge(wmNew, wmBefore)
	allDrafts = append(allDrafts, fired...)
	sortDrafts(allDrafts)

	if err := e.commitBarrier(allDrafts, wmNew); err != nil {
		e.failed = true
		return 0, err
	}
	return acceptedN, nil
}

// AdvanceWatermark 用源侧进度信号（心跳/空闲）推进水位线并触发到期窗口。
func (e *Engine) AdvanceWatermark(progressMs int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.failed {
		return fmt.Errorf("engine in failed state; restore from checkpoint required")
	}
	progress := time.UnixMilli(progressMs)
	wmNew := e.wm.Advance(progress)
	drafts := e.fireAndPurge(wmNew, wmNew)
	if err := e.commitBarrier(drafts, wmNew); err != nil {
		e.failed = true
		return err
	}
	return nil
}

// commitBarrier 统一分配 Seq、原子检查点、按序投递输出。
//
// 提交协议（崩溃恢复后不重复、不丢输出）：
//  1. 草稿按全局顺序分配 Seq；
//  2. 连同 pending（已写检查点未确认）一起原子写入检查点 —— 先持久化后投递；
//  3. 检查点成功后按序 Emit（Sink 对 Seq 幂等）；
//  4. Emit 失败：记录留在 pending（已在检查点中），本次返回错误；恢复或下次
//     屏障会重投，Sink 按 Seq 去重，不会产生重复效果；
//  5. 全部投递成功后做一次“确认检查点”清空 pending（失败也无害：重投被去重）。
func (e *Engine) commitBarrier(drafts []outDraft, wm time.Time) error {
	// 修订去重由 windowState.dirty 保证：仅当新的迟到事件被实际接受时置位，
	// 修正输出后立即清除；重复投递（相同事件 ID）不会再次置位。
	records := make([]sink.Record, 0, len(drafts))
	for _, d := range drafts {
		r := sink.Record{
			Seq: e.nextOutputSeq, Key: d.key, WindowID: d.windowID,
			WindowKind: string(d.kind), IsCorrection: d.correction,
			Value: d.value, Reason: d.reason,
		}
		records = append(records, r)
		e.nextOutputSeq++
		if st := e.lookupWindow(d.key, d.windowID); st != nil {
			st.lastSeq = r.Seq
			st.lastValue = d.value
		}
		e.log.Log(nil, 0, "output staged",
			"seq", r.Seq, "key", r.Key, "window_id", r.WindowID,
			"correction", r.IsCorrection, "value", r.Value, "reason", r.Reason)
	}

	// 待确认集合 = 之前遗留 pending + 本批新记录。
	pending := make([]checkpoint.PendingOutput, 0, len(e.pending)+len(records))
	pending = append(pending, e.pending...)
	nowMs := time.Now().UnixMilli()
	for _, r := range records {
		pending = append(pending, checkpoint.PendingOutput{
			Seq: r.Seq, Key: r.Key, WindowID: r.WindowID, Kind: r.WindowKind,
			IsCorrection: r.IsCorrection, Value: r.Value, EmittedAtUnixMs: nowMs,
		})
	}

	if e.opts.Store != nil {
		if err := e.saveCheckpoint(wm, pending, e.nextOutputSeq); err != nil {
			return err
		}
	}
	e.pending = pending

	// 按序投递；同时把之前遗留的 pending 全部幂等重投（Sink 按 Seq 去重）。
	if e.opts.Sink != nil {
		redo := make([]sink.Record, 0, len(pending))
		for _, p := range pending {
			redo = append(redo, pendingToRecord(p))
		}
		// 新记录在前（它们顺序连续），遗留 pending 随后；Seq 去重保证不重复。
		ordered := make([]sink.Record, 0, len(records)+len(e.pending))
		ordered = append(ordered, records...)
		for _, p := range e.pending {
			ordered = append(ordered, pendingToRecord(p))
		}
		_ = redo
		for _, r := range ordered {
			if err := e.opts.Sink.Emit(r); err != nil {
				e.log.Error("sink emit failed; output retained in pending", "seq", r.Seq, "err", err)
				return fmt.Errorf("%w: seq=%d: %v", ErrSinkFailed, r.Seq, err)
			}
		}
	}

	// 全部确认：推进 committedSeq。
	// 输出已在前面的检查点中持久化（pending 集合），崩溃重放由 Sink 按 Seq 去重；
	// 因此无需在成功路径再写一次检查点，下次屏障/显式 Checkpoint 会自然清空 pending。
	e.committedSeq = e.nextOutputSeq - 1
	e.pending = nil
	return nil
}

func pendingToRecord(p checkpoint.PendingOutput) sink.Record {
	return sink.Record{
		Seq: p.Seq, Key: p.Key, WindowID: p.WindowID, WindowKind: p.Kind,
		IsCorrection: p.IsCorrection, Value: p.Value,
	}
}

// lookupWindow 在分片中查找窗口状态（调用方持有 e.mu）。
func (e *Engine) lookupWindow(key, id string) *windowState {
	sh := e.shards[shardForKey(key, len(e.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if ks, ok := sh.keys[key]; ok {
		return ks.windows[id]
	}
	return nil
}
