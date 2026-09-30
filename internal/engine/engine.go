// Package engine 编排事件摄取、窗口分配、水位线推进、窗口触发、
// 去重、迟到修正、检查点与恢复。
//
// 确定性保证：
//   - 事件处理分两阶段：先在主循环按源顺序完成时间分类与水位线观测
//     （水位线只取决于事件时间序列），再把事件按分组键哈希分发到
//     多个 worker 并发写入窗口状态；同键事件始终在同一 worker 保序；
//   - 窗口触发只发生在屏障点：一个微批全部写完后，以水位线为准、
//     按 (Group, Window.Start, Window.End, Seq) 确定性顺序统一触发，
//     因此并发执行与串行执行产生完全相同的输出序列；
//   - 输出 Seq 在引擎内单调分配，同序重放输入必得同序输出；
//   - 检查点原子写入，恢复后水位线不回退、已确认输出不重发、
//     未完成窗口不丢失。
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/source"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/watermark"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// Config 是引擎全部可配置项。
type Config struct {
	Window          window.Spec
	Aggregator      string
	Lag             time.Duration // 水位线乱序容忍
	Late            event.LatePolicy
	AllowedLateness time.Duration // LateInclude 的宽限期（水位线越过 End 后仍可修正）
	Invalid         event.InvalidTimePolicy
	FutureBound     time.Duration // 事件时间相对处理时刻允许超前的最大幅度
	State           state.Config
	CheckpointDir   string
	CheckpointEvery int64 // 每处理 N 条做一次检查点；<=0 仅在收尾时
	Concurrency     int   // 分组处理并发度；输出与串行等价
}

// Engine 流式聚合引擎。
type Engine struct {
	cfg Config
	log *slog.Logger
	now func() time.Time

	mu         sync.Mutex
	sinkMu     sync.Mutex             // 保护并发 worker 对 sink 的旁路写入
	mergeLocks map[string]*sync.Mutex // 会话合并按分组键互斥
	store      state.Store
	wm         watermark.Watermark
	gen        *watermark.Generator
	assigner   window.Assigner
	reg        *aggregate.Registry
	nextSeq    int64
	srcOffset  int64
	sessionSeq int64 // 兼容旧检查点的单调计数器；窗口标识不再依赖它
	// pendingRetracts 是本屏障内会话合并产生的待撤销结果。
	// 只在屏障触发阶段按确定性键序统一发射、发射后清空，
	// 因此是屏障内瞬时状态：不进检查点、不无界增长，
	// 崩溃后随状态回退与输入重放被等价重建。
	retractMu       sync.Mutex
	pendingRetracts []pendingRetract

	stats Stats
}

// Option 配置引擎可选项。
type Option func(*Engine)

// WithLogger 设置结构化日志记录器。
func WithLogger(l *slog.Logger) Option { return func(e *Engine) { e.log = l } }

// WithClock 注入处理时钟（测试确定性用）。
func WithClock(now func() time.Time) Option { return func(e *Engine) { e.now = now } }

// New 创建引擎（不启动处理）。
func New(cfg Config, opts ...Option) (*Engine, error) {
	asg, err := window.NewAssigner(cfg.Window)
	if err != nil {
		return nil, fmt.Errorf("window config: %w", err)
	}
	store, err := state.New(cfg.State)
	if err != nil {
		return nil, fmt.Errorf("state store: %w", err)
	}
	if cfg.Aggregator == "" {
		cfg.Aggregator = "sum_count"
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	e := &Engine{
		cfg:        cfg,
		log:        slog.Default(),
		now:        time.Now,
		store:      store,
		gen:        watermark.NewGenerator(cfg.Lag),
		assigner:   asg,
		reg:        aggregate.NewRegistry(),
		mergeLocks: map[string]*sync.Mutex{},
	}
	for _, o := range opts {
		o(e)
	}
	if _, ok := e.reg.Get(cfg.Aggregator); !ok {
		return nil, fmt.Errorf("unknown aggregator %q", cfg.Aggregator)
	}
	return e, nil
}

// prepared 是预分类后的事件及其写入时可见的水位线。
type prepared struct {
	ev event.Event
	wm time.Time // 本批写入统一可见的水位线
}

// Run 从 src 消费事件并向 dst 输出，直到源正常结束、ctx 取消或源中断。
// 断点续传：先从检查点恢复 srcOffset/水位线/状态，已确认输出不重复发出。
func (e *Engine) Run(ctx context.Context, src source.Source, dst sink.Sink) (err error) {
	if e.cfg.CheckpointDir != "" {
		cs, cerr := checkpoint.NewFileStore(e.cfg.CheckpointDir)
		if cerr != nil {
			return cerr
		}
		if rerr := e.Restore(ctx, cs, dst); rerr != nil && !errors.Is(rerr, checkpoint.ErrNotFound) {
			return rerr
		}
	}

	ch, serr := src.Subscribe(ctx, e.srcOffset)
	if serr != nil {
		return fmt.Errorf("subscribe: %w", serr)
	}

	var sinceCheckpoint int64
	// barrierSize 只依赖输入中的绝对记录位置（微批大小或检查点周期），
	// 与"在哪里中断/恢复"无关；恢复后续跑的屏障序列与不间断运行对齐。
	barrierSize := int64(microBatch)
	if e.cfg.CheckpointEvery > 0 && e.cfg.CheckpointEvery < barrierSize {
		barrierSize = e.cfg.CheckpointEvery
	}
	flush := func(batch []source.Record) (bool, error) {
		if len(batch) == 0 {
			return false, nil
		}
		// 阶段一（按源顺序、串行）：时间分类、异常记录、观测事件时间。
		ready := make([]prepared, 0, len(batch))
		lastOffset := int64(-1)
		for _, rec := range batch {
			p, keep, err := e.prepare(rec.Event, dst)
			if err != nil {
				return false, err
			}
			lastOffset = rec.Offset
			if keep {
				ready = append(ready, p)
			}
		}
		// 阶段二（屏障内并发）：按分组键分片写入窗口状态。
		if err := e.writeBatch(ctx, ready, dst); err != nil {
			return false, err
		}
		// 屏障：统一触发，顺序确定。
		if err := e.trigger(ctx, dst); err != nil {
			return false, err
		}
		e.mu.Lock()
		e.srcOffset = lastOffset + 1
		e.stats.Processed += int64(len(batch))
		sinceCheckpoint += int64(len(batch))
		doCheckpoint := e.cfg.CheckpointEvery > 0 && sinceCheckpoint >= e.cfg.CheckpointEvery
		e.mu.Unlock()
		if doCheckpoint {
			if err := e.checkpoint(ctx, dst); err != nil {
				return false, err
			}
			sinceCheckpoint = 0
		}
		return true, nil
	}

	// 先把通道内记录收集完毕，再得知结束原因：
	// 这样屏障批次的"完整性"判定先于任何处理——若来源中断，
	// 末尾不足一个屏障的残余批次整批不处理，源偏移保持在上一屏障末尾，
	// 恢复后从源重新读取。不存在半更新状态，输出序列与不间断运行一致。
	var records []source.Record
	for rec := range ch {
		records = append(records, rec)
	}
	srcErr := src.Err()

	// 只处理"完整屏障"；正常结束时末尾残余批次作为最终屏障处理。
	consumed := int64(0)
	for consumed+barrierSize <= int64(len(records)) {
		if _, err := flush(records[consumed : consumed+barrierSize]); err != nil {
			return err
		}
		consumed += barrierSize
	}
	if srcErr != nil {
		// 中断：残余事件未提交（srcOffset 已停在最后一个完整屏障）。
		return srcErr
	}
	// 正常结束：处理残余批次并推进水位线触发所有剩余窗口。
	if _, err := flush(records[consumed:]); err != nil {
		return err
	}
	if terr := e.flushAll(ctx, dst); terr != nil {
		return terr
	}
	if cerr := e.checkpoint(ctx, dst); cerr != nil {
		return cerr
	}
	return nil
}

// prepare 执行写入前的确定性预处理：
// 时间异常分类与记录、观测事件时间推进水位线。
// 返回处理后的事件与批水位线；keep=false 表示事件被拒绝（仅留记录）。
func (e *Engine) prepare(ev event.Event, dst sink.Sink) (prepared, bool, error) {
	now := e.now()
	e.mu.Lock()
	wm := e.wm.Get()
	e.mu.Unlock()

	if reason := event.ClassifyTime(ev, wm, now, e.cfg.FutureBound); reason != "" {
		resolved, action := event.ResolveInvalid(ev, e.cfg.Invalid, wm, now)
		d := event.Decision{
			EventID: ev.ID, Key: ev.Key, Ts: ev.Ts,
			Reason: reason, Detail: "classified invalid; action=" + string(action),
		}
		e.log.Warn("invalid event time",
			"event_id", ev.ID, "key", ev.Key, "ts", ev.Ts, "reason", reason, "action", action)
		if err := dst.DeliverSide(sink.SideRecord{Decision: d, Event: ev}); err != nil {
			return prepared{}, false, err
		}
		e.bumpStat(func(s *Stats) { s.Rejected++ })
		if e.cfg.Invalid == event.InvalidReject {
			return prepared{}, false, nil
		}
		ev = resolved
	}

	e.mu.Lock()
	proposed := e.gen.Observe(e.wm.Get(), ev.Ts)
	if proposed.After(e.wm.Get()) {
		if e.wm.Advance(proposed) {
			e.log.Info("watermark advanced", "watermark", proposed, "event_id", ev.ID, "event_ts", ev.Ts)
		}
	}
	curWm := e.wm.Get()
	if e.cfg.Window.Kind == window.Session {
		// 保留单调计数器仅为兼容旧检查点中的 SessionSeq 字段；
		// 会话窗口标识不再依赖它（Seq 恒为 0），与到达顺序无关。
		e.sessionSeq++
	}
	e.mu.Unlock()

	return prepared{ev: ev, wm: curWm}, true, nil
}

// writeEvent 把已预处理事件写入其所属窗口；按水位线判定迟到并执行策略。
// 同分组键的调用被分片机制串行化，因此无需额外加锁即可保序。
func (e *Engine) writeEvent(ctx context.Context, p prepared, dst sink.Sink) error {
	ev, curWm := p.ev, p.wm
	wins := e.assigner.Assign(ev.Ts)
	for _, w := range wins {
		late := e.wm.IsSet() && ev.Ts.Before(curWm)
		k := state.Key{Group: ev.Key, Win: w}
		if late {
			switch e.cfg.Late {
			case event.LateDrop:
				e.recordSide(dst, ev, curWm, w, event.ReasonLateDropped, "event behind watermark; dropped")
				e.bumpStat(func(s *Stats) { s.LateHandled++ })
				continue
			case event.LateSideOutput:
				e.recordSide(dst, ev, curWm, w, event.ReasonLateSideOutput, "event behind watermark; side output")
				e.bumpStat(func(s *Stats) { s.LateHandled++ })
				continue
			case event.LateInclude:
				if curWm.After(w.End.Add(e.cfg.AllowedLateness)) {
					e.recordSide(dst, ev, curWm, w, event.ReasonLateDropped, "late beyond allowed lateness; dropped")
					e.bumpStat(func(s *Stats) { s.LateHandled++ })
					continue
				}
			}
		}

		if w.Kind == window.Session {
			if err := e.applySession(ctx, k, ev, late); err != nil {
				return err
			}
		} else {
			en, err := e.store.GetOrCreate(ctx, k, e.cfg.Aggregator)
			if err != nil {
				return err
			}
			if !en.Acc.Add(ev) {
				e.logDuplicate(ev, w, curWm)
				e.bumpStat(func(s *Stats) { s.Duplicates++ })
				continue
			}
			if late {
				en.Dirty = true
				e.bumpStat(func(s *Stats) { s.LateHandled++ })
				e.log.Info("late event included",
					"event_id", ev.ID, "key", ev.Key, "window", window.WindowID(w),
					"watermark", curWm, "event_ts", ev.Ts)
			}
		}
	}
	return nil
}

func (e *Engine) bumpStat(f func(*Stats)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	f(&e.stats)
}

// deliverSide 串行化并发 worker 对旁路输出的写入。
func (e *Engine) deliverSide(dst sink.Sink, r sink.SideRecord) error {
	e.sinkMu.Lock()
	defer e.sinkMu.Unlock()
	return dst.DeliverSide(r)
}

func (e *Engine) recordSide(dst sink.Sink, ev event.Event, wm time.Time, w window.Window, reason event.Reason, detail string) {
	d := event.Decision{
		EventID: ev.ID, Key: ev.Key, Ts: ev.Ts, Reason: reason,
		Detail: fmt.Sprintf("%s (window=%s, watermark=%s)", detail, window.WindowID(w), wm.Format(time.RFC3339Nano)),
	}
	e.log.Warn("side decision",
		"event_id", ev.ID, "key", ev.Key, "reason", reason, "window", window.WindowID(w), "watermark", wm)
	_ = e.deliverSide(dst, sink.SideRecord{Decision: d, Event: ev})
}

func (e *Engine) logDuplicate(ev event.Event, w window.Window, wm time.Time) {
	e.log.Info("duplicate event ignored",
		"event_id", ev.ID, "key", ev.Key, "window", window.WindowID(w), "watermark", wm)
}

// Stats 暴露运行统计（测试/可观测性）。
type Stats struct {
	Processed   int64
	Duplicates  int64
	LateHandled int64
	Rejected    int64
	Checkpoints int64
	Spills      int64
	Retractions int64 // 已发出的撤销条数
	Watermark   time.Time
}

// Stats 返回当前统计快照。
func (e *Engine) Stats() Stats {
	spills := e.store.SpillCount()
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.stats
	s.Spills = spills
	s.Watermark = e.wm.Get()
	return s
}
