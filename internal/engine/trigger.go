package engine

import (
	"context"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// trigger 是屏障点：当前事件批次处理完且水位线可能推进后调用。
// 以确定性键序遍历全部窗口，触发规则：
//   - 非会话窗口：watermark >= End 即触发；首版 INSERT，之后在允许
//     迟到宽限期内因迟到事件产生的更新为 CORRECTION；
//   - 会话窗口：先按 Gap 聚类合并，再触发 watermark >= End 的窗口；
//   - 宽限期彻底越过（watermark > End+AllowedLateness）后关闭并可清理，
//     此后迟到事件不再修正主输出（由 processOne 记为旁路）。
//
// 全部判定与输出顺序只依赖状态与水位线，故并发输入与串行输入输出一致。
func (e *Engine) trigger(ctx context.Context, dst sink.Sink) error {
	e.mu.Lock()
	curWm := e.wm.Get()
	gap := e.cfg.Window.Gap
	allowed := e.cfg.AllowedLateness
	e.mu.Unlock()
	if curWm.IsZero() {
		return nil
	}

	// 会话窗口在触发前做最终合并兜底（分组逐个合并，组间顺序无关）。
	if e.cfg.Window.Kind == window.Session {
		groups := map[string]struct{}{}
		keys, err := e.store.ListKeys(ctx)
		if err != nil {
			return err
		}
		for _, k := range keys {
			if k.Win.Kind == window.Session {
				groups[k.Group] = struct{}{}
			}
		}
		for g := range groups {
			if err := e.mergeGroup(ctx, g, gap); err != nil {
				return err
			}
		}
	}

	keys, err := e.store.ListKeys(ctx)
	if err != nil {
		return err
	}
	now := e.now()
	for _, k := range keys {
		en, ok, err := e.store.Get(ctx, k)
		if err != nil || !ok {
			return err
		}
		fire := !curWm.Before(k.Win.End) // watermark >= End
		if !fire {
			continue
		}

		// 已发出过：仅当自上次发出后有迟到更新（Count 变化无法直接得知，
		// 因此用 Acc 与 Revision 对照——迟到写入会把 entry 标记 dirty）。
		kind := aggregate.Insert
		revision := int64(1)
		if en.Emitted {
			if !en.Dirty {
				// 无新迟到事件：跳过，不重复输出。
			} else {
				kind = aggregate.Correction
				revision = en.Revision + 1
			}
		}
		if en.Emitted && !en.Dirty {
			// 关闭判定（无论是否本次发出）。
			if curWm.After(k.Win.End.Add(allowed)) && !en.Closed {
				_ = e.store.Close(k)
			}
			continue
		}

		out := aggregate.Output{
			Revision:  revision,
			Kind:      kind,
			Key:       k.Group,
			Window:    k.Win,
			Value:     en.Acc.Value,
			Count:     en.Acc.Count,
			EmitAt:    now,
			Watermark: curWm,
			Late:      kind == aggregate.Correction,
		}
		if err := e.emit(ctx, dst, &out, k, en); err != nil {
			return err
		}
		e.log.Info("window fired",
			"key", k.Group, "window", window.WindowID(k.Win),
			"kind", kind.String(), "revision", revision,
			"value", out.Value, "count", out.Count,
			"watermark", curWm, "basis", "watermark>=window_end")

		// 宽限期彻底越过则关闭。
		if curWm.After(k.Win.End.Add(allowed)) {
			_ = e.store.Close(k)
		}
	}

	// 清理彻底过期窗口，状态占用保持有界。
	if _, err := e.store.Purge(curWm.Add(-allowed).UnixNano()); err != nil {
		return err
	}
	// 注意：Purge 只删 Closed+Emitted 且 End < curWm-allowed 的窗口。
	_ = time.Time{}
	return nil
}

// emit 分配 Seq、投递并把"已发出"标记与输出原子绑定：
// 先投递，投递成功后才 MarkEmitted；投递失败则不标记，重试不丢。
func (e *Engine) emit(ctx context.Context, dst sink.Sink, out *aggregate.Output, k state.Key, en *state.Entry) error {
	e.mu.Lock()
	e.nextSeq++
	out.Seq = e.nextSeq
	e.mu.Unlock()

	// 恢复场景：sink 中已存在相同 Seq 之后的确认输出时，NextSeq 已被对齐；
	// Deliver 自身按 Seq 幂等，重复投递不会产生重复结果。
	if err := dst.Deliver(*out); err != nil {
		return err
	}
	en.Dirty = false
	return e.store.MarkEmitted(k, out.Revision)
}

// flushAll 在源正常结束后触发所有剩余窗口。
// 采用"推进到最大已观测事件时间"而非 +Inf，保持结果可解释且确定。
func (e *Engine) flushAll(ctx context.Context, dst sink.Sink) error {
	e.mu.Lock()
	maxTs := e.gen.MaxObserved()
	e.mu.Unlock()
	if !maxTs.IsZero() {
		e.mu.Lock()
		e.wm.Advance(maxTs.Add(time.Nanosecond))
		e.mu.Unlock()
	}
	if err := e.trigger(ctx, dst); err != nil {
		return err
	}
	// 会话窗口的 End 依赖最晚事件；水位线到 maxTs 后仍可能有
	// End==maxTs+Gap 的窗口未触发，再推进一次其 End。
	keys, err := e.store.ListKeys(ctx)
	if err != nil {
		return err
	}
	var maxEnd time.Time
	for _, k := range keys {
		if k.Win.End.After(maxEnd) {
			maxEnd = k.Win.End
		}
	}
	if !maxEnd.IsZero() {
		e.mu.Lock()
		e.wm.Advance(maxEnd)
		e.mu.Unlock()
		return e.trigger(ctx, dst)
	}
	return nil
}
