// Package window 定义窗口类型与确定的窗口归属计算。
//
// 所有窗口均为半开区间 [Start, End)：边界时刻的事件归属于后一个窗口。
// 窗口边界基于 epoch（或显式 Offset）对齐，因此同一事件时间无论以何种
// 到达顺序进入引擎，窗口归属都完全相同。
package window

import (
	"fmt"
	"time"
)

// Kind 窗口种类。
type Kind int

const (
	Tumbling Kind = iota
	Sliding
	Session
)

func (k Kind) String() string {
	switch k {
	case Tumbling:
		return "tumbling"
	case Sliding:
		return "sliding"
	case Session:
		return "session"
	default:
		return "unknown"
	}
}

// Window 是一个半开时间区间 [Start, End)。
type Window struct {
	Kind  Kind
	Start time.Time
	End   time.Time
	// 仅会话窗口使用：生成序号，用于区分同一时间范围但不同合并批次产生的会话。
	// 取合并成员中的最小 Seq，保证重放结果稳定。
	Seq int64
}

// Contains 判断事件时间是否落在窗口内（左端闭、右端开）。
func (w Window) Contains(ts time.Time) bool {
	return !ts.Before(w.Start) && ts.Before(w.End)
}

// Spec 是窗口的可序列化配置。
type Spec struct {
	Kind   Kind
	Size   time.Duration // 滚动窗口长度 / 滑动窗口长度
	Slide  time.Duration // 滑动步长（滑动窗口）
	Gap    time.Duration // 会话间隔（会话窗口）
	Offset time.Duration // 相对 epoch 的对齐偏移，需小于窗口长度
}

// Assigner 为事件分配它所属的窗口。
// 滚动：恰好一个；滑动：一个或多个；会话：初始一个，后续由状态层合并。
type Assigner interface {
	Assign(ts time.Time) []Window
	Spec() Spec
}

// floorDiv 返回不超过 a/b 的最大整数（b>0）。
func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b) != 0 && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// tumblingAssigner 滚动窗口分配器。
type tumblingAssigner struct{ spec Spec }

func (a tumblingAssigner) Spec() Spec { return a.spec }

func (a tumblingAssigner) Assign(ts time.Time) []Window {
	n := ts.UnixNano() + int64(a.spec.Offset)
	startN := floorDiv(n, int64(a.spec.Size))*int64(a.spec.Size) - int64(a.spec.Offset)
	start := time.Unix(0, startN)
	return []Window{{Kind: Tumbling, Start: start, End: start.Add(a.spec.Size)}}
}

// slidingAssigner 滑动窗口分配器。
type slidingAssigner struct{ spec Spec }

func (a slidingAssigner) Spec() Spec { return a.spec }

func (a slidingAssigner) Assign(ts time.Time) []Window {
	size, slide := int64(a.spec.Size), int64(a.spec.Slide)
	n := ts.UnixNano() + int64(a.spec.Offset)
	// 最后一个 Start <= ts 的窗口起点。
	lastStartN := floorDiv(n, slide)*slide - int64(a.spec.Offset)
	// 第一个满足 Start+Size > ts（即能包含 ts）的窗口起点：
	// 需要严格 Start > ts-Size，故最小 slide 对齐起点
	// = (floor((ts-Size)/slide) + 1) * slide
	firstStartN := (floorDiv(n-size, slide)+1)*slide - int64(a.spec.Offset)
	var out []Window
	for s := firstStartN; s <= lastStartN; s += slide {
		start := time.Unix(0, s)
		out = append(out, Window{Kind: Sliding, Start: start, End: start.Add(a.spec.Size)})
	}
	return out
}

// sessionAssigner 会话窗口初始分配器：单事件会话 [ts, ts+Gap)。
// 真正的会话合并发生在状态层触发时调用 MergeSessions。
type sessionAssigner struct{ spec Spec }

func (a sessionAssigner) Spec() Spec { return a.spec }

func (a sessionAssigner) Assign(ts time.Time) []Window {
	return []Window{{Kind: Session, Start: ts, End: ts.Add(a.spec.Gap)}}
}

// NewAssigner 依据 Spec 创建确定性分配器；非法配置直接报错，
// 避免运行时出现静默的错误归属。
func NewAssigner(s Spec) (Assigner, error) {
	switch s.Kind {
	case Tumbling:
		if s.Size <= 0 {
			return nil, fmt.Errorf("tumbling window requires positive size, got %v", s.Size)
		}
		if s.Offset < 0 || s.Offset >= s.Size {
			return nil, fmt.Errorf("offset %v out of range [0,%v)", s.Offset, s.Size)
		}
		return tumblingAssigner{spec: s}, nil
	case Sliding:
		if s.Size <= 0 || s.Slide <= 0 {
			return nil, fmt.Errorf("sliding window requires positive size and slide")
		}
		if s.Offset < 0 || s.Offset >= s.Slide {
			return nil, fmt.Errorf("offset %v out of range [0,%v)", s.Offset, s.Slide)
		}
		return slidingAssigner{spec: s}, nil
	case Session:
		if s.Gap <= 0 {
			return nil, fmt.Errorf("session window requires positive gap")
		}
		return sessionAssigner{spec: s}, nil
	default:
		return nil, fmt.Errorf("unknown window kind %d", s.Kind)
	}
}

// WindowID 返回窗口的稳定标识（同 Key 下唯一）。
func WindowID(w Window) string {
	return fmt.Sprintf("%s:%d:%d:%d", w.Kind, w.Start.UnixNano(), w.End.UnixNano(), w.Seq)
}
