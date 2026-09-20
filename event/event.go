package event

import (
	"errors"
	"fmt"
	"time"
)

// Policy 决定时间信息缺失或明显异常的事件如何处理。
type Policy int

const (
	// PolicyReject：拒绝异常事件（记录后不进入聚合，绝不静默丢弃）。
	PolicyReject Policy = iota
	// PolicyAssign：使用注入时钟为异常事件分配事件时间后继续处理。
	PolicyAssign
)

func (p Policy) String() string {
	switch p {
	case PolicyReject:
		return "reject"
	case PolicyAssign:
		return "assign-current-time"
	default:
		return fmt.Sprintf("unknown(%d)", int(p))
	}
}

// Event 是进入引擎的单个事件。
type Event struct {
	ID        string
	Key       string
	Timestamp time.Time // 事件时间；零值表示缺失
	Value     int64
}

// Reason 记录事件时间异常的判定依据。
type Reason int

const (
	ReasonOK Reason = iota
	ReasonMissingTime
	ReasonBeforeMinTime
	ReasonAfterFutureTolerance
)

func (r Reason) String() string {
	switch r {
	case ReasonOK:
		return "ok"
	case ReasonMissingTime:
		return "missing-event-time"
	case ReasonBeforeMinTime:
		return "event-time-before-min-allowed"
	case ReasonAfterFutureTolerance:
		return "event-time-exceeds-future-tolerance"
	default:
		return fmt.Sprintf("unknown(%d)", int(r))
	}
}

// ErrEventRejected 表示事件按时间策略被拒绝。
var ErrEventRejected = errors.New("event rejected by timestamp policy")

// Clock 提供注入式的当前时间，便于测试。
type Clock func() time.Time

// TimeValidator 校验事件时间，并按策略处理缺失/异常时间。
// 判定顺序：缺失 -> 早于最小允许时间 -> 超出未来容忍上限。
type TimeValidator struct {
	policy      Policy
	minTime     time.Time
	futureSlack time.Duration
	now         Clock
}

// NewTimeValidator 创建时间校验器。
// minTime 为零值时不做下界检查；futureSlack<=0 时不做上界检查；now 为 nil 时用 time.Now。
func NewTimeValidator(policy Policy, minTime time.Time, futureSlack time.Duration, now Clock) *TimeValidator {
	if now == nil {
		now = time.Now
	}
	return &TimeValidator{policy: policy, minTime: minTime, futureSlack: futureSlack, now: now}
}

// Validate 返回（可能被修正时间的）事件、判定原因、是否被拒绝。
// 被拒绝时调用方必须记录 Reason，不得静默丢弃。
func (v *TimeValidator) Validate(e Event) (Event, Reason, bool) {
	reason := ReasonOK
	switch {
	case e.Timestamp.IsZero():
		reason = ReasonMissingTime
	case !v.minTime.IsZero() && e.Timestamp.Before(v.minTime):
		reason = ReasonBeforeMinTime
	case v.futureSlack > 0 && e.Timestamp.After(v.now().Add(v.futureSlack)):
		reason = ReasonAfterFutureTolerance
	}
	if reason == ReasonOK {
		return e, ReasonOK, false
	}
	if v.policy == PolicyReject {
		return e, reason, true
	}
	// PolicyAssign：统一用注入时钟重新分配事件时间，判定依据保留在原因里供记录。
	e.Timestamp = v.now()
	return e, reason, false
}
