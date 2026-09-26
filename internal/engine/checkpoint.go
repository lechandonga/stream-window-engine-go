package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// checkpoint 写出一次一致性快照：水位线、Seq、源偏移、全部窗口状态。
// 顺序：先持久化状态快照本身（FileStore.Save 原子 rename），
// 失败则引擎状态不变、已提交结果不受影响。
func (e *Engine) checkpoint(ctx context.Context, dst sink.Sink) error {
	if e.cfg.CheckpointDir == "" {
		return nil
	}
	cs, err := checkpoint.NewFileStore(e.cfg.CheckpointDir)
	if err != nil {
		return err
	}

	e.mu.Lock()
	snap := checkpoint.Snapshot{
		Version:      1,
		Watermark:    e.wm.Get(),
		NextSeq:      e.nextSeq,
		SourceOffset: e.srcOffset,
		CreatedAt:    e.now(),
	}
	e.mu.Unlock()

	entries, err := e.store.SnapshotEntries(ctx)
	if err != nil {
		return fmt.Errorf("snapshot state: %w", err)
	}
	for _, se := range entries {
		var seen []string
		var seenVals []int64
		if se.En.Acc != nil {
			seen = se.En.Acc.SeenIDs()
			seenVals = se.En.Acc.SeenValues()
		}
		es := checkpoint.EntrySnapshot{
			Group:    se.K.Group,
			Kind:     int(se.K.Win.Kind),
			Start:    se.K.Win.Start,
			End:      se.K.Win.End,
			Seq:      se.K.Win.Seq,
			Value:    se.En.Acc.Value,
			Count:    se.En.Acc.Count,
			Revision: se.En.Revision,
			Emitted:  se.En.Emitted,
			Closed:   se.En.Closed,
			SeenIDs:  seen,
			// 与 SeenIDs 同序，确保恢复后 Value 可正确重算。
			SeenValues:   seenVals,
			MaxEventNano: se.En.MaxEvent.UnixNano(),
		}
		for _, r := range se.En.Pending {
			es.Pending = append(es.Pending, checkpoint.RetractionSnapshot{
				ResultID: r.ResultID,
				Revision: r.Revision,
				Start:    r.Win.Start,
				End:      r.Win.End,
				Seq:      r.Win.Seq,
				Value:    r.Value,
				Count:    r.Count,
				Late:     r.Late,
			})
		}
		snap.Entries = append(snap.Entries, es)
	}
	snap.SpillFiles = e.store.SpillFiles()

	if err := cs.Save(snap); err != nil {
		return err
	}
	e.mu.Lock()
	e.stats.Checkpoints++
	e.mu.Unlock()
	e.log.Info("checkpoint saved",
		"watermark", snap.Watermark, "offset", snap.SourceOffset,
		"next_seq", snap.NextSeq, "entries", len(snap.Entries))
	return nil
}

// Restore 从检查点恢复水位线、Seq、源偏移与全部窗口状态。
// 规则：
//   - 无检查点（ErrNotFound）：从零启动；
//   - 检查点损坏：原样返回 ErrCorrupt，绝不带着猜测状态继续；
//   - 已确认输出不重发：sink 的 LastSeq 与快照 NextSeq 对齐取较大值；
//   - 水位线只增不减：若当前（注入）水位线更高则保留更高值。
func (e *Engine) Restore(ctx context.Context, cs checkpoint.Store, dst sink.Sink) error {
	snap, err := cs.Load()
	if err != nil {
		return err
	}

	entries := make([]state.SnapshotEntry, 0, len(snap.Entries))
	var maxSessionSeq int64
	for _, es := range snap.Entries {
		acc := aggregateNew(e.cfg.Aggregator)
		acc.Value = es.Value
		acc.Count = es.Count
		acc.LoadSeen(es.SeenIDs, es.SeenValues)
		w := window.Window{
			Kind:  window.Kind(es.Kind),
			Start: es.Start,
			End:   es.End,
			Seq:   es.Seq,
		}
		if w.Kind == window.Session && es.Seq > maxSessionSeq {
			maxSessionSeq = es.Seq
		}
		var pending []state.Retraction
		for _, r := range es.Pending {
			pending = append(pending, state.Retraction{
				ResultID: r.ResultID,
				Revision: r.Revision,
				Win:      window.Window{Kind: window.Kind(es.Kind), Start: r.Start, End: r.End, Seq: r.Seq},
				Value:    r.Value,
				Count:    r.Count,
				Late:     r.Late,
			})
		}
		var maxEvent time.Time
		if es.MaxEventNano != 0 {
			maxEvent = time.Unix(0, es.MaxEventNano)
		}
		entries = append(entries, state.SnapshotEntry{
			K: state.Key{Group: es.Group, Win: w},
			En: state.Entry{
				Acc: acc, Revision: es.Revision,
				Emitted: es.Emitted, Closed: es.Closed,
				MaxEvent: maxEvent,
				Pending:  pending,
			},
		})
	}
	if err := e.store.RestoreEntries(ctx, entries); err != nil {
		return fmt.Errorf("restore state: %w", err)
	}

	e.mu.Lock()
	e.wm = watermarkFrom(snap.Watermark)
	e.gen = generatorFrom(snap.Watermark, e.cfg.Lag)
	e.srcOffset = snap.SourceOffset
	e.sessionSeq = maxSessionSeq
	// NextSeq：以 sink 已确认值与快照值的较大者为准，杜绝重复 Seq。
	if dst != nil {
		if ls := dst.LastSeq(); ls >= snap.NextSeq {
			e.nextSeq = ls
		} else {
			e.nextSeq = snap.NextSeq
		}
	} else {
		e.nextSeq = snap.NextSeq
	}
	e.mu.Unlock()

	e.log.Info("restored from checkpoint",
		"watermark", snap.Watermark, "offset", snap.SourceOffset,
		"next_seq", e.nextSeq, "entries", len(snap.Entries))
	return nil
}
