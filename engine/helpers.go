package engine

import (
	"hash/fnv"
	"strings"

	"github.com/lechandonga/stream-window-engine-go/window"
)

// windowID 生成窗口的稳定标识；会话窗口仅用开始时间（结束时间可能因迟到事件延长）。
func windowID(w window.Window, k window.Kind) string {
	if k == window.Session {
		// 保留前缀与起始毫秒，结束时间不参与，保证会话延长/合并后 ID 仍稳定。
		return string(k) + "[" + ms(w.Start) + ",)"
	}
	return w.ID(k)
}

func ms(t interface{ UnixMilli() int64 }) string {
	v := t.UnixMilli()
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// spillRefFromWindowID 由窗口 ID 推导确定性的落盘文件名（重启后可重新定位）。
func spillRefFromWindowID(id string, k window.Kind) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return "spill-" + string(k) + "-" + strings.ReplaceAll(strings.ReplaceAll(id, "/", "_"), " ", "_") +
		"-" + formatHex(h.Sum64()) + ".json"
}

func formatHex(v uint64) string {
	const digits = "0123456789abcdef"
	var b [16]byte
	for i := 15; i >= 0; i-- {
		b[i] = digits[v&0xf]
		v >>= 4
	}
	return string(b[:])
}

// shardForKey 用 FNV 哈希把 key 稳定映射到分片。
func shardForKey(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

// msTime 统一的 unix 毫秒取整（零值时间保持 0）。
func msTime(t interface{ UnixMilli() int64 }) int64 { return t.UnixMilli() }
