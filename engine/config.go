package engine

import (
	"time"

	"github.com/lechandonga/stream-window-engine-go/aggregate"
)

// WindowType 窗口类型配置。
type WindowType string

const (
	WindowTumbling WindowType = "tumbling"
	WindowSliding  WindowType = "sliding"
	WindowSession  WindowType = "session"
)

// LatePolicy 水位线之后到达事件的处理策略。
type LatePolicy string

const (
	// LateDrop：丢弃迟到事件并记录（结果不变，记录动作=late-drop）。
	LateDrop LatePolicy = "drop"
	// LateRevise：在允许迟到时长内纳入，并向下游发出修正输出；
	// 超过允许迟到时长则记录为 late-drop-expired（与普通丢弃可区分）。
	LateRevise LatePolicy = "revise"
	// LateRedirect：转入侧输出（不影响主结果，记录动作=late-redirect）。
	LateRedirect LatePolicy = "redirect"
)

// OverflowPolicy 状态达到上限时的策略。
type OverflowPolicy string

const (
	// OverflowBlock：背压，ProcessEvent 返回 ErrStateLimit（由上游重试）。
	OverflowBlock OverflowPolicy = "block"
	// OverflowSpill：将最旧（最早结束）的窗口状态落盘，事件再到该窗口时懒加载。
	OverflowSpill OverflowPolicy = "spill"
)

// Config 是引擎的全部可配置项。
type Config struct {
	// 窗口
	WindowType WindowType
	WindowSize time.Duration // tumbling / sliding
	Slide      time.Duration // sliding（必须整除 WindowSize）
	SessionGap time.Duration // session：相邻事件时间差 > gap 即断开
	Aggregate  aggregate.Kind

	// 水位线与迟到
	WatermarkLag    time.Duration // 水位线 = 已观测最大事件时间 - lag
	AllowedLateness time.Duration // 窗口 End+AllowedLateness 之后不再接受修订
	LateEventPolicy LatePolicy

	// 时间异常事件：true=拒绝并记录；false=用注入时钟分配时间后处理（仍记录依据）
	RejectInvalidTime bool
	MinEventTime      time.Time     // 零值=不做下界检查
	FutureTolerance   time.Duration // <=0=不做未来时间检查

	// 状态上限（按分片均摊；总上限不超过 MaxWindows 向上对齐到分片数）
	MaxWindows int
	OnOverflow OverflowPolicy
	SpillDir   string

	// 检查点
	CheckpointDir    string
	CheckpointEveryN int // 每处理 N 个事件触发一次屏障检查点；0=仅在批次/水位线推进与关闭时

	// 并发
	ShardCount int
}

// DefaultConfig 返回一套可用的默认配置：10s 滚动窗口、sum、水位线滞后 1s、
// 允许迟到 5s（修订策略）、内存上限 1000 窗口、超出背压、单分片。
func DefaultConfig() Config {
	return Config{
		WindowType:       WindowTumbling,
		WindowSize:       10 * time.Second,
		Aggregate:        aggregate.Sum,
		WatermarkLag:     time.Second,
		AllowedLateness:  5 * time.Second,
		LateEventPolicy:  LateRevise,
		MaxWindows:       1000,
		OnOverflow:       OverflowBlock,
		CheckpointEveryN: 0,
		ShardCount:       1,
	}
}
