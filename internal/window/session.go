package window

import (
	"sort"
	"time"
)

// MergeSessions 合并彼此间隔不超过 gap 的会话窗口。
// 合并规则（确定且顺序无关）：
//   - 先按 Start 升序排序（Start 相同按 End、再按 Seq 排序）；
//   - 每个单事件初始会话为 [ts, ts+gap)，因此 prev.End 即前一事件的
//     时间加上 gap；prev.End >= cur.Start 等价于相邻事件时间差 <= gap，
//     恰好等于 gap（首尾相接）合并，超过 gap（哪怕 1ns）保持独立；
//   - 合并后 End 取较大值；会话窗口的 Seq 恒为 0，标识只由区间决定，
//     与到达顺序无关。
//
// 输入任意排列，输出始终相同。
func MergeSessions(ws []Window, gap time.Duration) []Window {
	if len(ws) == 0 {
		return nil
	}
	sorted := make([]Window, len(ws))
	copy(sorted, ws)
	sort.SliceStable(sorted, func(i, j int) bool {
		if !sorted[i].Start.Equal(sorted[j].Start) {
			return sorted[i].Start.Before(sorted[j].Start)
		}
		if !sorted[i].End.Equal(sorted[j].End) {
			return sorted[i].End.Before(sorted[j].End)
		}
		return sorted[i].Seq < sorted[j].Seq
	})

	merged := []Window{sorted[0]}
	for _, cur := range sorted[1:] {
		prev := &merged[len(merged)-1]
		if prev.End.UnixNano() >= cur.Start.UnixNano() {
			if cur.End.After(prev.End) {
				prev.End = cur.End
			}
			prev.Seq = 0
		} else {
			merged = append(merged, cur)
		}
	}
	return merged
}
