package engine

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lechandonga/stream-window-engine-go/aggregate"
	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/event"
	"github.com/lechandonga/stream-window-engine-go/sink"
	"github.com/lechandonga/stream-window-engine-go/source"
	"github.com/lechandonga/stream-window-engine-go/watermark"
	"github.com/lechandonga/stream-window-engine-go/window"
)

// 引擎对外暴露的、可区分的失败原因。
var (
	ErrInvalidConfig     = errors.New("invalid engine configuration")
	ErrStateLimit        = errors.New("state limit reached (backpressure)")
	ErrInvalidEventTime  = event.ErrEventRejected
	ErrCheckpointFailed  = checkpoint.ErrWriteFailed
	ErrCheckpointCorrupt = checkpoint.ErrCorrupt
	ErrSourceFailed      = source.ErrSourceInterrupted
	ErrSinkFailed        = sink.ErrSinkUnavailable
	ErrSpill             = ErrSpillFailed
)

// InvalidEventRecord 记录时间异常事件的判定依据与处置。
type InvalidEventRecord struct {
	EventID string `json:"event_id"`
	Key     string `json:"key"`
	Reason  string `json:"reason"`
	Policy  string `json:"policy"`
}

// LateEventRecord 记录迟到事件的判定依据与处置（不同策略/动作必须可区分）。
type LateEventRecord struct {
	EventID   string `json:"event_id"`
	Key       string `json:"key"`
	Watermark int64  `json:"watermark_unix_ms"`
	EventTime int64  `json:"event_time_unix_ms"`
	WindowID  string `json:"window_id"`
	Policy    string `json:"policy"`
	Action    string `json:"action"` // late-drop / late-drop-expired / late-revise / late-redirect
}

// SideOutputRecord 是迟到事件被重定向（redirect 策略）后的侧输出记录。
type SideOutputRecord struct {
	EventID   string `json:"event_id"`
	Key       string `json:"key"`
	EventTime int64  `json:"event_time_unix_ms"`
	Value     int64  `json:"value"`
	Watermark int64  `json:"watermark_unix_ms"`
}

// Options 构造引擎的依赖。
type Options struct {
	Source source.Source
	Sink   sink.Sink
	Store  checkpoint.Store
	Now    event.Clock // 注入时钟；nil 用 time.Now
	Logger *slog.Logger
}

// Engine 流式事件聚合引擎。
type Engine struct {
	cfg       Config
	opts      Options
	assigner  window.Assigner
	validator *event.TimeValidator

	mu     sync.Mutex // 保护下列跨分片状态与屏障串行化
	wm     *watermark.Generator
	shards []*shard
	spill  *spillManager

	nextOutputSeq int64
	committedSeq  int64
	pending       []checkpoint.PendingOutput

	invalidEvents []InvalidEventRecord
	latedEvents   []LateEventRecord
	sideOutputs   []SideOutputRecord

	eventCount int64
	restored   bool
	closed     bool
	failed     bool
	log        *slog.Logger
}

// shard 负责一组 key 的窗口状态；分片间并行，屏障处全局排序保证确定性。
type shard struct {
	id     int
	engine *Engine
	mu     sync.Mutex
	keys   map[string]*keyState
	maxWin int // 本分片内存窗口数上限
}

// New 创建引擎（校验配置；不做恢复）。
func New(cfg Config, opts Options) (*Engine, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	var assigner window.Assigner
	switch cfg.WindowType {
	case WindowTumbling:
		assigner = window.NewTumblingAssigner(cfg.WindowSize)
	case WindowSliding:
		assigner = window.NewSlidingAssigner(cfg.WindowSize, cfg.Slide)
	case WindowSession:
		assigner = window.NewSessionAssigner(cfg.SessionGap)
	default:
		return nil, fmt.Errorf("%w: unknown window type %q", ErrInvalidConfig, cfg.WindowType)
	}
	policy := event.PolicyAssign
	if cfg.RejectInvalidTime {
		policy = event.PolicyReject
	}
	e := &Engine{
		cfg:           cfg,
		opts:          opts,
		assigner:      assigner,
		validator:     event.NewTimeValidator(policy, cfg.MinEventTime, cfg.FutureTolerance, opts.Now),
		wm:            watermark.NewGenerator(cfg.WatermarkLag),
		nextOutputSeq: 1,
		log:           opts.Logger,
	}
	n := cfg.ShardCount
	if n < 1 {
		n = 1
	}
	perShard := (cfg.MaxWindows + n - 1) / n
	if perShard < 1 {
		perShard = 1
	}
	e.shards = make([]*shard, n)
	for i := range e.shards {
		e.shards[i] = &shard{id: i, engine: e, keys: map[string]*keyState{}, maxWin: perShard}
	}
	spillDir := cfg.SpillDir
	if spillDir == "" && cfg.OnOverflow == OverflowSpill {
		spillDir = cfg.CheckpointDir
	}
	e.spill = newSpillManager(spillDir, cfg.Aggregate, nil)
	return e, nil
}

func validateConfig(cfg Config) error {
	switch cfg.WindowType {
	case WindowTumbling:
		if cfg.WindowSize <= 0 {
			return fmt.Errorf("%w: tumbling window requires positive WindowSize", ErrInvalidConfig)
		}
	case WindowSliding:
		if cfg.WindowSize <= 0 || cfg.Slide <= 0 || cfg.WindowSize%cfg.Slide != 0 {
			return fmt.Errorf("%w: sliding window requires positive size/slide and slide divides size", ErrInvalidConfig)
		}
	case WindowSession:
		if cfg.SessionGap <= 0 {
			return fmt.Errorf("%w: session window requires positive SessionGap", ErrInvalidConfig)
		}
	default:
		return fmt.Errorf("%w: unknown window type %q", ErrInvalidConfig, cfg.WindowType)
	}
	if _, err := aggregate.New(cfg.Aggregate); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if cfg.WatermarkLag < 0 {
		return fmt.Errorf("%w: WatermarkLag must be >= 0", ErrInvalidConfig)
	}
	if cfg.AllowedLateness < 0 {
		return fmt.Errorf("%w: AllowedLateness must be >= 0", ErrInvalidConfig)
	}
	if cfg.MaxWindows < 0 {
		return fmt.Errorf("%w: MaxWindows must be >= 0", ErrInvalidConfig)
	}
	switch cfg.LateEventPolicy {
	case LateDrop, LateRevise, LateRedirect:
	default:
		return fmt.Errorf("%w: unknown late policy %q", ErrInvalidConfig, cfg.LateEventPolicy)
	}
	switch cfg.OnOverflow {
	case OverflowBlock, OverflowSpill:
	default:
		return fmt.Errorf("%w: unknown overflow policy %q", ErrInvalidConfig, cfg.OnOverflow)
	}
	if cfg.OnOverflow == OverflowSpill && cfg.SpillDir == "" && cfg.CheckpointDir == "" {
		return fmt.Errorf("%w: spill policy requires SpillDir or CheckpointDir", ErrInvalidConfig)
	}
	return nil
}

// InvalidEvents 返回时间异常事件处置记录（含被拒绝/被修正时间的事件）。
func (e *Engine) InvalidEvents() []InvalidEventRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]InvalidEventRecord, len(e.invalidEvents))
	copy(out, e.invalidEvents)
	return out
}

// LateEvents 返回迟到事件判定与处置记录。
func (e *Engine) LateEvents() []LateEventRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]LateEventRecord, len(e.latedEvents))
	copy(out, e.latedEvents)
	return out
}

// SideOutputs 返回 redirect 策略下的迟到事件侧输出。
func (e *Engine) SideOutputs() []SideOutputRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]SideOutputRecord, len(e.sideOutputs))
	copy(out, e.sideOutputs)
	return out
}
