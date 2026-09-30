package engine

import (
	"context"
	"fmt"

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
		SessionSeq:   e.sessionSeq,
		CreatedAt:    e.now(),
	}
	e.mu.Unlock()

	entries, err := e.store.SnapshotEntries(ctx)
	if err != nil {
		return fmt.Errorf("snapshot state: %w", err)
	}
	for _, se := range entries {
		var seen []string
		if se.En.Acc != nil {
			seen = se.En.Acc.SeenIDs()
		}
		snap.Entries = append(snap.Entries, checkpoint.EntrySnapshot{
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
		})
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
	maxSessionSeq := int64(0)
	for _, es := range snap.Entries {
		acc := aggregateNew(e.cfg.Aggregator)
		acc.Value = es.Value
		acc.Count = es.Count
		acc.LoadSeen(es.SeenIDs)
		w := window.Window{
			Kind:  window.Kind(es.Kind),
			Start: es.Start,
			End:   es.End,
			Seq:   es.Seq,
		}
		if w.Kind == window.Session {
			// 会话标识只由 (分组, 区间) 决定：归一化旧快照中按到达
			// 顺序分配的非零 Seq，保证续跑与全新重放标识一致。
			if es.Seq > maxSessionSeq {
				maxSessionSeq = es.Seq
			}
			w.Seq = 0
		}
		entries = append(entries, state.SnapshotEntry{
			K: state.Key{Group: es.Group, Win: w},
			En: state.Entry{
				Acc: acc, Revision: es.Revision,
				Emitted: es.Emitted, Closed: es.Closed,
			},
		})
	}
	if err := e.store.RestoreEntries(ctx, entries); err != nil {
		return fmt.Errorf("restore state: %w", err)
	}

	// 会话序号：优先用快照中的计数器本身；旧快照无该字段时
	// 回退到从存活条目反推（兼容旧格式）。
	sessionSeq := snap.SessionSeq
	if sessionSeq < maxSessionSeq {
		sessionSeq = maxSessionSeq
	}

	e.mu.Lock()
	e.wm = watermarkFrom(snap.Watermark)
	e.gen = generatorFrom(snap.Watermark, e.cfg.Lag)
	e.srcOffset = snap.SourceOffset
	e.sessionSeq = sessionSeq
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
