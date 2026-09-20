package watermark

import "time"

// Generator 基于事件时间单调推进水位线。
// 不变量：Current() 在任何情况下都不回退（ObservedMax 单调，水位线 = max - lag）。
type Generator struct {
	current     time.Time
	observedMax time.Time
	lag         time.Duration
	initialized bool
}

// NewGenerator 创建水位线生成器。lag 表示水位线落后于观察到的最大事件时间的固定时长。
func NewGenerator(lag time.Duration) *Generator {
	return &Generator{lag: lag}
}

func (g *Generator) advanceTo(wm time.Time) time.Time {
	if !g.initialized || wm.After(g.current) {
		g.current = wm
	}
	g.initialized = true
	return g.current
}

// Observe 观测一个事件时间：ObservedMax 取最大值，水位线 = ObservedMax - lag。
// 迟到（更早）的事件时间不会使水位线回退。
func (g *Generator) Observe(ts time.Time) time.Time {
	if !g.initialized || ts.After(g.observedMax) {
		g.observedMax = ts
	}
	return g.advanceTo(g.observedMax.Add(-g.lag))
}

// Advance 用源侧进度时间直接推进水位线（心跳 / 空闲源 / 显式注入）。
// progress 被视为“源已观察到的最大进度”，同样只升不降。
func (g *Generator) Advance(progress time.Time) time.Time {
	return g.Observe(progress)
}

// Current 返回当前水位线。
func (g *Generator) Current() time.Time { return g.current }

// ObservedMax 返回已观测到的最大事件时间。
func (g *Generator) ObservedMax() time.Time { return g.observedMax }

// IsInitialized 返回是否已观测到任何进度。
func (g *Generator) IsInitialized() bool { return g.initialized }

// Restore 在重启后恢复水位线内部状态，恢复值之后水位线仍不得回退。
// 检查点只持久化水位线本身；observedMax 至少恢复为 wm+lag，使后续推进连续。
func (g *Generator) Restore(wm, observedMax time.Time) {
	g.initialized = true
	if wm.After(g.current) {
		g.current = wm
	}
	minObserved := wm.Add(g.lag)
	if observedMax.After(minObserved) {
		g.observedMax = observedMax
	} else {
		g.observedMax = minObserved
	}
}
