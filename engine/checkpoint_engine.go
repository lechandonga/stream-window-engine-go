package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/sink"
)

const engineStateVersion = 1

// saveCheckpoint 构造快照并原子写入。失败时不修改任何内存状态（返回错误后
// 引擎进入 failed 态，必须从最近成功的检查点恢复）。
func (e *Engine) saveCheckpoint(wm time.Time, pending []checkpoint.PendingOutput, nextSeq int64) error {
	keyStates := map[string]json.RawMessage{}
	for _, sh := range e.shards {
		sh.mu.Lock()
		keys := make([]string, 0, len(sh.keys))
		for k := range sh.keys {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			raw, err := sh.keys[k].marshal(e.cfg.Aggregate, e.assigner.Kind())
			if err != nil {
				sh.mu.Unlock()
				return err
			}
			keyStates[k] = raw
		}
		sh.mu.Unlock()
	}

	st := &checkpoint.State{
		Version:         engineStateVersion,
		WatermarkUnixMs: wm.UnixMilli(),
		KeyStates:       keyStates,
		NextOutputSeq:   nextSeq,
		CommittedSeq:    e.committedSeq,
		Pending:         pending,
		SpillFiles:      e.spill.registeredFiles(),
		CreatedAtUnixMs: e.opts.Now().UnixMilli(),
	}
	e.log.Info("checkpoint saving",
		"watermark_ms", wm.UnixMilli(), "keys", len(keyStates),
		"next_seq", nextSeq, "pending", len(pending), "spill_files", len(st.SpillFiles))
	if err := e.opts.Store.Save(st); err != nil {
		return fmt.Errorf("%w: %v", ErrCheckpointFailed, err)
	}
	// 检查点持久化成功后，落盘文件的生命周期决定才生效。
	e.spill.commit()
	if err := e.spill.applyPendingDeletes(); err != nil {
		return err
	}
	return nil
}

// Checkpoint 在当前水位线执行一次显式屏障检查点。
func (e *Engine) Checkpoint() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.saveCheckpoint(e.wm.Current(), e.pending, e.nextOutputSeq)
}

// Restore 从检查点恢复水位线、窗口状态、输出游标与待确认输出，
// 并重放 pending（Sink 幂等去重）。不存在检查点时是合法的首次启动。
func (e *Engine) Restore() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.opts.Store == nil {
		e.restored = true
		return nil
	}
	st, err := e.opts.Store.Load()
	if errors.Is(err, checkpoint.ErrNoCheckpoint) {
		e.log.Info("no checkpoint found, starting fresh")
		e.restored = true
		return nil
	}
	if err != nil {
		// ErrCorrupt / 读取错误：直接返回可区分的失败原因，不启动半状态引擎。
		return err
	}
	if st.Version != engineStateVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrCheckpointCorrupt, st.Version)
	}

	// 恢复水位线（水位线在任何情况下都不得回退：Restore 内部取 max）。
	wm := time.UnixMilli(st.WatermarkUnixMs)
	e.wm.Restore(wm, wm.Add(e.cfg.WatermarkLag))

	// 恢复 spill 管理器登记，并清理磁盘上的孤儿 spill 文件。
	e.spill = newSpillManager(e.spill.dir, e.cfg.Aggregate, st.SpillFiles)
	if e.spill.dir != "" {
		if err := e.spill.reconcileOrphans(); err != nil {
			return err
		}
	}

	// 恢复各 key 窗口状态到正确分片。
	for k, raw := range st.KeyStates {
		ks, err := unmarshalKeyState(raw, e.cfg.Aggregate, e.assigner.Kind())
		if err != nil {
			return fmt.Errorf("%w: restore key %s: %v", ErrCheckpointCorrupt, k, err)
		}
		sh := e.shards[shardForKey(k, len(e.shards))]
		sh.mu.Lock()
		sh.keys[k] = ks
		for _, wst := range ks.windows {
			if wst.spilledRef == "" {
			}
		}
		sh.mu.Unlock()
	}

	// 恢复输出游标。以 Sink 实际已生效位置为准（防止“检查点已确认但 Sink
	// 未持久化”的不一致），只前推不回退。
	e.nextOutputSeq = st.NextOutputSeq
	e.committedSeq = st.CommittedSeq
	e.pending = append(e.pending, st.Pending...)
	if e.opts.Sink != nil {
		if last, ok := e.opts.Sink.LastSeq(); ok && last > e.committedSeq {
			e.committedSeq = last
		}
	}

	// 重放待确认输出（按 Seq 排序；Sink 去重保证不重复）。
	sort.Slice(e.pending, func(i, j int) bool { return e.pending[i].Seq < e.pending[j].Seq })
	if e.opts.Sink != nil {
		for _, p := range e.pending {
			r := pendingToRecord(p)
			if err := e.opts.Sink.Emit(r); err != nil {
				return fmt.Errorf("%w: replay pending seq=%d: %v", ErrSinkFailed, p.Seq, err)
			}
		}
		// 重放成功后写确认检查点；失败无害（下次再重放，去重生效）。
		e.committedSeq = e.nextOutputSeq - 1
		e.pending = nil
		if err := e.saveCheckpoint(e.wm.Current(), nil, e.nextOutputSeq); err != nil {
			e.log.Warn("post-restore ack checkpoint failed (harmless)", "err", err)
			e.pending = st.Pending
		}
	}

	e.restored = true
	e.log.Info("restored from checkpoint",
		"watermark_ms", st.WatermarkUnixMs, "keys", len(st.KeyStates),
		"next_seq", e.nextOutputSeq, "committed_seq", e.committedSeq,
		"replayed_pending", len(st.Pending))
	return nil
}

// Shutdown 优雅停止：在当前水位线触发到期窗口并做最后一次检查点。
// 尚未完成的窗口保留在检查点中，重启后不丢失。
func (e *Engine) Shutdown() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	drafts := e.fireAndPurge(e.wm.Current(), e.wm.Current())
	if err := e.commitBarrier(drafts, e.wm.Current()); err != nil {
		return err
	}
	if e.opts.Store != nil {
		if err := e.saveCheckpoint(e.wm.Current(), e.pending, e.nextOutputSeq); err != nil {
			return err
		}
	}
	e.log.Info("engine shutdown",
		"watermark_ms", e.wm.Current().UnixMilli(),
		"committed_seq", e.committedSeq)
	return nil
}

var _ sink.Record
