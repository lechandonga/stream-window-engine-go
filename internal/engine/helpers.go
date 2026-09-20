package engine

import (
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/watermark"
)

// aggregateNew 按名称创建空累加器（未知名称回退默认）。
func aggregateNew(name string) *aggregate.Accumulator {
	reg := aggregate.NewRegistry()
	agg, ok := reg.Get(name)
	if !ok {
		agg = aggregate.SumCount{}
	}
	return agg.Init()
}

func watermarkFrom(ts time.Time) watermark.Watermark {
	return watermark.FromTime(ts)
}

// generatorFrom 用恢复的水位线反推生成器初始状态：
// 以 (watermark+lag) 作为已观测最大时间，保证水位线不会因重放而回退。
func generatorFrom(wm time.Time, lag time.Duration) *watermark.Generator {
	g := watermark.NewGenerator(lag)
	if !wm.IsZero() {
		g.Observe(time.Time{}, wm.Add(lag))
	}
	return g
}
