// Package event 定义流式事件、时间异常处理策略与判定记录。
package event

import "time"

// Event 是进入引擎的最小事件单元。
// Key 为分组键；ID 用于去重（重复投递保护）；
// Ts 为事件时间；Value 参与聚合。
type Event struct {
	ID    string
	Key   string
	Ts    time.Time
	Value int64
}

// HasTime 报告事件是否携带事件时间。
func (e Event) HasTime() bool { return !e.Ts.IsZero() }

// InvalidTimePolicy 决定缺失或明显异常事件时间的处理方式。
// 无论哪种策略，异常事件都会写入 DecisionLog，绝不静默丢弃。
type InvalidTimePolicy int

const (
	// InvalidReject 拒绝事件：不参与聚合，仅记录。
	InvalidReject InvalidTimePolicy = iota
	// InvalidAssignWatermark 以当前水位线时间作为事件时间。
	InvalidAssignWatermark
	// InvalidAssignProcessingTime 以处理时刻（墙上时钟）作为事件时间。
	InvalidAssignProcessingTime
)

// Reason 描述事件被特殊处理的原因。
type Reason string

const (
	ReasonMissingTime       Reason = "missing_event_time"
	ReasonTimeOutOfRange    Reason = "time_out_of_allowed_range"
	ReasonFutureBeyondBound Reason = "event_time_beyond_future_bound"
	ReasonLateDropped       Reason = "late_event_dropped"
	ReasonLateSideOutput    Reason = "late_event_side_output"
	ReasonLateIncluded      Reason = "late_event_included"
	ReasonDuplicate         Reason = "duplicate_redelivery"
)

// Decision 记录单个事件的处置依据，保证结果可解释。
type Decision struct {
	EventID string
	Key     string
	Ts      time.Time
	Reason  Reason
	Detail  string
}

// LatePolicy 决定水位线之后到达事件的处置策略。
type LatePolicy int

const (
	// LateDrop 丢弃迟到事件并记录（不修正输出）。
	LateDrop LatePolicy = iota
	// LateSideOutput 迟到事件进入侧输出，不修正主输出。
	LateSideOutput
	// LateInclude 在允许迟到宽限内纳入聚合并产生 CORRECTION。
	LateInclude
)
