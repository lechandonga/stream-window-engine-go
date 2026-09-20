package event

import "time"

// EarliestPlausibleTime 是可接受的最早事件时间：早于该时刻视为异常。
// 取 2000-01-01，足以覆盖测试数据又能拦截明显损坏的时间戳。
var EarliestPlausibleTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// ClassifyTime 判定事件时间是否缺失或明显异常。
// 返回空字符串表示时间正常；否则返回对应 Reason（调用方必须记录）。
//   - wm：当前水位线（仅用于解释，不参与未来界判定）
//   - now：当前处理时刻
//   - futureBound：事件时间相对 now 允许超前的最大幅度
func ClassifyTime(e Event, wm, now time.Time, futureBound time.Duration) Reason {
	if !e.HasTime() {
		return ReasonMissingTime
	}
	if e.Ts.Before(EarliestPlausibleTime) {
		return ReasonTimeOutOfRange
	}
	if futureBound >= 0 && e.Ts.After(now.Add(futureBound)) {
		return ReasonFutureBeyondBound
	}
	return ""
}

// ResolveInvalid 按策略处理时间异常事件，返回处理后的事件与判定原因。
// reason 必非空（调用方负责写入判定日志）。
//   - InvalidReject：原样返回，调用方不得将其纳入聚合；
//   - InvalidAssignWatermark：水位线已设置则用水位线时间，否则用 now；
//   - InvalidAssignProcessingTime：使用 now。
func ResolveInvalid(e Event, p InvalidTimePolicy, wm, now time.Time) (Event, Reason) {
	switch p {
	case InvalidAssignWatermark:
		if !wm.IsZero() {
			e.Ts = wm
		} else {
			e.Ts = now
		}
		return e, Reason("assigned_watermark_time")
	case InvalidAssignProcessingTime:
		e.Ts = now
		return e, Reason("assigned_processing_time")
	default: // InvalidReject
		return e, Reason("rejected_invalid_time")
	}
}
