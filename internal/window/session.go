package window

import (
	"sort"
	"time"
)

// MergeSessions 合并彼此间隔不超过 gap 的会话窗口。
// 合并规则（确定且顺序无关）：
//   - 先按 Start 升序排序（Start 相同按 End、再按 Seq 排序）；
//   - 相邻窗口若 prev.End+gap >= cur.Start（即间隔 <= gap，含首尾相接）则合并；
//   - 合并后 End 取较大值；Seq 取成员中的最小 Seq。
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

	gapN := int64(gap)
	merged := []Window{sorted[0]}
	for _, cur := range sorted[1:] {
		prev := &merged[len(merged)-1]
		if prev.End.UnixNano()+gapN >= cur.Start.UnixNano() {
			if cur.End.After(prev.End) {
				prev.End = cur.End
			}
			if cur.Seq < prev.Seq {
				prev.Seq = cur.Seq
			}
		} else {
			merged = append(merged, cur)
		}
	}
	return merged
}
