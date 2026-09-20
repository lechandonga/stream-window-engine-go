package window

import (
	"fmt"
	"time"
)

// Kind 标识窗口类型。
type Kind string

const (
	Tumbling Kind = "tumbling"
	Sliding  Kind = "sliding"
	Session  Kind = "session"
)

// Window 是一个 [Start, End) 时间区间（左闭右开，边界确定可解释）。
type Window struct {
	Start time.Time
	End   time.Time
}

// Contains 判断事件时间是否落在窗口内（左闭右开）。
func (w Window) Contains(ts time.Time) bool {
	return (ts.Equal(w.Start) || ts.After(w.Start)) && ts.Before(w.End)
}

// ID 是窗口的稳定标识（同一窗口跨重启必须得到相同 ID）。
func (w Window) ID(kind Kind) string {
	return fmt.Sprintf("%s[%d,%d)", kind, w.Start.UnixMilli(), w.End.UnixMilli())
}

// Assigner 决定某个事件时间归属哪些窗口。
type Assigner interface {
	Kind() Kind
	Assign(ts time.Time) []Window
}

// floor 将 ts 对齐到不晚于它的、epoch 起算的 step 边界。
func floor(ts time.Time, step time.Duration) time.Time {
	ms := ts.UnixMilli()
	st := step.Milliseconds()
	return time.UnixMilli(ms - ((ms%st)+st)%st).In(ts.Location())
}

// TumblingAssigner 按固定大小、对齐 Unix epoch 的滚动窗口分配。
// 边界：[floor(ts,size), +size)，每个事件恰好属于一个窗口。
type TumblingAssigner struct {
	size time.Duration
}

func NewTumblingAssigner(size time.Duration) *TumblingAssigner {
	return &TumblingAssigner{size: size}
}

func (a *TumblingAssigner) Kind() Kind { return Tumbling }

func (a *TumblingAssigner) Assign(ts time.Time) []Window {
	start := floor(ts, a.size)
	return []Window{{Start: start, End: start.Add(a.size)}}
}

// SlidingAssigner 滑动窗口：窗口大小 size，滑动步长 slide（slide 整除 size）。
// 归属规则：事件时间 ts 属于所有满足 start <= ts < start+size 的对齐窗口，
// 数量恰为 size/slide，顺序按开始时间升序，保证输出确定。
type SlidingAssigner struct {
	size  time.Duration
	slide time.Duration
}

func NewSlidingAssigner(size, slide time.Duration) *SlidingAssigner {
	return &SlidingAssigner{size: size, slide: slide}
}

func (a *SlidingAssigner) Kind() Kind { return Sliding }

func (a *SlidingAssigner) Assign(ts time.Time) []Window {
	lastStart := floor(ts, a.slide)
	n := int(a.size / a.slide)
	windows := make([]Window, 0, n)
	for i := n - 1; i >= 0; i-- {
		start := lastStart.Add(-time.Duration(i) * a.slide)
		windows = append(windows, Window{Start: start, End: start.Add(a.size)})
	}
	return windows
}
