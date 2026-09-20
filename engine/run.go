package engine

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/lechandonga/stream-window-engine-go/source"
)

// Run 从源持续拉取批次直到源正常结束（io.EOF）或 ctx 取消。
//
// 行为：
//   - 每个 EventBatch 经 ProcessBatch 处理；Progress 非零时推进水位线
//     （用于心跳/空闲源，保证窗口最终能触发）；
//   - 来源中断（ErrSourceInterrupted）原样向上返回可区分错误，不做静默吞掉；
//   - CheckpointEveryN>0 时按处理事件数插入屏障检查点。
//
// Run 从给定源拉取；src 为 nil 时使用 Options.Source。
func (e *Engine) Run(ctx context.Context, srcs ...source.Source) error {
	src := e.opts.Source
	if len(srcs) > 0 && srcs[0] != nil {
		src = srcs[0]
	}
	if src == nil {
		return errors.New("Run requires a source")
	}
	if !e.restored {
		if err := e.Restore(); err != nil {
			return err
		}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		batch, err := src.Poll()
		if errors.Is(err, io.EOF) {
			e.log.Info("source exhausted, run finished")
			return nil
		}
		if errors.Is(err, source.ErrSourceInterrupted) {
			e.log.Error("source interrupted", "err", err)
			return fmt.Errorf("%w: %v", ErrSourceFailed, err)
		}
		if err != nil {
			return err
		}
		if len(batch.Events) > 0 {
			if _, err := e.ProcessBatch(batch.Events); err != nil {
				return err
			}
		}
		if !batch.Progress.IsZero() {
			if err := e.AdvanceWatermark(batch.Progress.UnixMilli()); err != nil {
				return err
			}
		}
		if e.cfg.CheckpointEveryN > 0 {
			e.mu.Lock()
			n := e.eventCount
			e.mu.Unlock()
			if n > 0 && int(n)%e.cfg.CheckpointEveryN == 0 {
				if err := e.Checkpoint(); err != nil {
					return err
				}
			}
		}
	}
}

// WatermarkMs 返回当前水位线的 unix 毫秒（仅观测用）。
func (e *Engine) WatermarkMs() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wm.Current().UnixMilli()
}

// CommittedSeq 返回已确认输出的最大 Seq。
func (e *Engine) CommittedSeq() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.committedSeq
}
