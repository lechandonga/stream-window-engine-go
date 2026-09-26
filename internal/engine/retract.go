package engine

import (
	"context"
	"sort"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// pendingRetract 记录一条待撤销的已发布结果：
// 会话合并把某个已发出的 (Key, Window) 并入更大的会话后，
// 旧标识的结果不再成立，必须在同一屏障内、替代结果之前明确撤销。
type pendingRetract struct {
	key        state.Key     // 被撤销结果的 (分组, 窗口)
	revision   int64         // 被撤销时已发出的修订号
	value      int64         // 被撤销时的聚合值（下游对账校验用）
	count      int64         // 被撤销时的事件数
	replacedBy window.Window // 替代结果的窗口（合并目标）
}

// noteRetract 登记一条待撤销（会话合并路径调用，可能并发，内部加锁）。
func (e *Engine) noteRetract(r pendingRetract) {
	e.retractMu.Lock()
	e.pendingRetracts = append(e.pendingRetracts, r)
	e.retractMu.Unlock()
}

// emitRetractions 在屏障触发阶段统一发射本屏障登记的全部撤销：
//   - 按 (Group, Window.Start, End, Seq) 确定性排序，与并发度无关；
//   - 撤销在正常窗口触发之前发出，保证下游先删旧记录再写替代结果；
//   - 与正常输出共享全局 Seq，sink 按 Seq 幂等，重放/重启不会重复生效；
//   - 发射成功后清空列表，撤销状态不随运行时间增长。
func (e *Engine) emitRetractions(ctx context.Context, dst sink.Sink, curWm time.Time) error {
	e.retractMu.Lock()
	pending := e.pendingRetracts
	e.pendingRetracts = nil
	e.retractMu.Unlock()
	if len(pending) == 0 {
		return nil
	}

	sort.Slice(pending, func(i, j int) bool {
		a, b := pending[i].key, pending[j].key
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		if !a.Win.Start.Equal(b.Win.Start) {
			return a.Win.Start.Before(b.Win.Start)
		}
		if !a.Win.End.Equal(b.Win.End) {
			return a.Win.End.Before(b.Win.End)
		}
		return a.Win.Seq < b.Win.Seq
	})

	now := e.now()
	for _, r := range pending {
		out := aggregate.Output{
			Kind:       aggregate.Retract,
			Key:        r.key.Group,
			Window:     r.key.Win,
			Revision:   r.revision,
			Value:      r.value,
			Count:      r.count,
			EmitAt:     now,
			Watermark:  curWm,
			ResultID:   aggregate.ResultID(r.key.Group, r.key.Win),
			ReplacedBy: aggregate.ResultID(r.key.Group, r.replacedBy),
		}
		e.mu.Lock()
		e.nextSeq++
		out.Seq = e.nextSeq
		e.mu.Unlock()
		// Deliver 按 Seq 幂等：重复投递（含重启重放）不会重复撤销。
		if err := dst.Deliver(out); err != nil {
			return err
		}
		e.bumpStat(func(s *Stats) { s.Retractions++ })
		e.log.Info("result retracted",
			"key", r.key.Group, "window", window.WindowID(r.key.Win),
			"revision", r.revision, "replaced_by", window.WindowID(r.replacedBy),
			"watermark", curWm, "basis", "session_merged")
	}
	return nil
}
