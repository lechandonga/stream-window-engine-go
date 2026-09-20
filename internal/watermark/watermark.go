// Package watermark 实现单调推进的水位线（watermark）。
//
// 语义：水位线 W(t) 表示系统相信"事件时间 <= W(t) 的事件均已到达"。
// 本引擎采用有界乱序生成方式：W = maxObservedEventTime - lag。
// 水位线只能前进、永不回退；即使后续观测到更旧的事件时间，
// 或从检查点恢复后重放历史事件，水位线也保持不变。
package watermark

import "time"

// Watermark 一旦推进绝不回退（零值表示"尚未产生水位线"）。
type Watermark struct {
	ts time.Time
}

// New 创建初始水位线（零值时间表示尚未观测到任何事件时间）。
func New() Watermark { return Watermark{} }

// FromTime 从检查点恢复水位线。
func FromTime(ts time.Time) Watermark { return Watermark{ts: ts} }

// Get 返回当前水位线时间；未设置时返回零值。
func (w Watermark) Get() time.Time { return w.ts }

// IsSet 报告水位线是否已被设置过。
func (w Watermark) IsSet() bool { return !w.ts.IsZero() }

// Advance 尝试推进水位线：仅当 ts 严格更新时前进并返回 true。
// 传入零值时间或更旧时间都不会改变水位线——任何情况下都不回退。
func (w *Watermark) Advance(ts time.Time) bool {
	if ts.IsZero() {
		return false
	}
	if ts.After(w.ts) {
		w.ts = ts
		return true
	}
	return false
}

// Generator 基于"已观测最大事件时间 - 乱序容忍 lag"生成水位线。
type Generator struct {
	maxTs    time.Time
	lag      time.Duration
	hasEvent bool
}

// NewGenerator 创建生成器；lag 为允许的乱序时间差（bounded out-of-orderness）。
func NewGenerator(lag time.Duration) *Generator {
	if lag < 0 {
		lag = 0
	}
	return &Generator{lag: lag}
}

// Observe 观测一个事件时间，返回建议的新水位线（可能等于旧值，
// 也可能为零值，表示尚不足以产生水位线）。调用方负责把建议值
// 交给 Watermark.Advance 做单调裁决。
func (g *Generator) Observe(cur time.Time, ts time.Time) time.Time {
	if ts.IsZero() {
		return cur
	}
	if !g.hasEvent || ts.After(g.maxTs) {
		g.maxTs = ts
		g.hasEvent = true
	}
	proposed := g.maxTs.Add(-g.lag)
	if proposed.After(cur) {
		return proposed
	}
	return cur
}

// MaxObserved 返回已观测的最大事件时间（未观测时为零值）。
func (g *Generator) MaxObserved() time.Time { return g.maxTs }

// HasEvents 报告是否已观测过任何有效事件时间。
func (g *Generator) HasEvents() bool { return g.hasEvent }
