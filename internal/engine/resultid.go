package engine

import (
	"fmt"

	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// ResultID 是逻辑结果的稳定标识，只依赖数据，不依赖运行：
// 同一份输入无论以何种顺序、并发度、是否中断恢复，同一逻辑结果
// （同一会话谱系、同一固定窗口）都得到相同的 ResultID。
//
//   - 滚动/滑动窗口的边界由事件时间按 epoch 对齐确定，
//     故直接用窗口的 [Start,End) 作为身份；
//   - 会话窗口的身份是"会话内最早事件时间"（即谱系起点）：
//     迟到事件延长会话末端、或多个会话合并时，该起点不变，
//     因此 INSERT -> CORRECTION/RETRACT 能用同一标识串起来。
//
// 会话窗口的 Start 在合并时取并集最小值（见 clusterSessions），
// 正是其成员中的最早事件时间，故直接以 Start 作为谱系身份。
func ResultID(group string, w window.Window) string {
	if w.Kind == window.Session {
		return fmt.Sprintf("session|%s|%d", group, w.Start.UnixNano())
	}
	return fmt.Sprintf("%s|%s|%d|%d", w.Kind.String(), group,
		w.Start.UnixNano(), w.End.UnixNano())
}

func resultIDOfKey(k state.Key) string { return ResultID(k.Group, k.Win) }
