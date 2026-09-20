package window

import "time"

// SessionAssigner 按活动间隔 gap 划分会话窗口。
// 会话的真实合并发生在引擎状态层（需要按 key 观察全部事件）；
// 分配器只负责给出事件的初始单元素会话 [ts, ts+gap)。
type SessionAssigner struct {
	gap time.Duration
}

func NewSessionAssigner(gap time.Duration) *SessionAssigner {
	return &SessionAssigner{gap: gap}
}

func (a *SessionAssigner) Kind() Kind { return Session }

// Assign 返回事件的初始会话窗口 [ts, ts+gap)。
func (a *SessionAssigner) Assign(ts time.Time) []Window {
	return []Window{{Start: ts, End: ts.Add(a.gap)}}
}

// Gap 返回会话活动间隔：相邻事件时间差 > gap 时断开为两个会话。
func (a *SessionAssigner) Gap() time.Duration { return a.gap }
