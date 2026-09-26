package engine

import (
	"context"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// emitRetractions 在替代结果之前逐条发出 en.Pending 中的撤销。
//
// 幂等性：
//   - 每条撤销携带自己的全局 Seq（sink 按 Seq 去重，重复投递/重放
//     不会产生重复撤销）；
//   - 撤销携带被撤销结果最后发布时的 (ResultID, Revision)，下游可校验
//     当前是否仍持有该版本，绝不误撤仍然有效的结果；
//   - 全部撤销投递成功后才清空 Pending 并落盘；若在投递途中失败，
//     队列保留，重试时整组重发，由 Seq 幂等收敛。
func (e *Engine) emitRetractions(ctx context.Context, dst sink.Sink, k state.Key, en *state.Entry, now time.Time, curWm time.Time) error {
	replacedBy := resultIDOfKey(k)
	for _, r := range en.Pending {
		out := aggregate.Output{
			Revision:   r.Revision,
			Kind:       aggregate.Retract,
			ResultID:   r.ResultID,
			Key:        k.Group,
			Window:     r.Win,
			Value:      r.Value,
			Count:      r.Count,
			ReplacedBy: replacedBy,
			EmitAt:     now,
			Watermark:  curWm,
			Late:       r.Late,
		}
		e.mu.Lock()
		e.nextSeq++
		out.Seq = e.nextSeq
		e.mu.Unlock()
		if err := dst.Deliver(out); err != nil {
			return err
		}
		e.bumpStat(func(s *Stats) { s.Retractions++ })
		e.log.Info("result retracted",
			"result_id", r.ResultID, "key", k.Group, "revision", r.Revision,
			"replaced_by", replacedBy, "window", window.WindowID(r.Win), "seq", out.Seq)
	}
	en.Pending = nil
	return e.store.Put(k, en)
}
