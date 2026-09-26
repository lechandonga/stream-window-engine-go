package engine

import (
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// ResultID 规则：固定窗口用边界、会话用最早事件时间；
// 与 Seq（合并批次）无关，重放/并发/重启下稳定。
func TestResultIDRules(t *testing.T) {
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tw := window.Window{Kind: window.Tumbling, Start: base, End: base.Add(10 * time.Second)}
	if got, want := ResultID("k", tw), ResultID("k", tw); got != want {
		t.Fatal("ResultID must be deterministic")
	}
	// 滚动窗口：不同边界不同标识。
	tw2 := window.Window{Kind: window.Tumbling, Start: base.Add(10 * time.Second), End: base.Add(20 * time.Second)}
	if ResultID("k", tw) == ResultID("k", tw2) {
		t.Fatal("distinct tumbling windows must have distinct ResultIDs")
	}
	// 会话：同一最早事件时间、不同 End（被迟到延长）与不同 Seq（合并批次）
	// 仍属同一谱系。
	s1 := window.Window{Kind: window.Session, Start: base, End: base.Add(10 * time.Second), Seq: 3}
	s2 := window.Window{Kind: window.Session, Start: base, End: base.Add(40 * time.Second), Seq: 1}
	if ResultID("k", s1) != ResultID("k", s2) {
		t.Fatal("session lineage id must survive extension/merge")
	}
	// 会话：不同起点是不同谱系。
	s3 := window.Window{Kind: window.Session, Start: base.Add(50 * time.Second), End: base.Add(60 * time.Second), Seq: 1}
	if ResultID("k", s1) == ResultID("k", s3) {
		t.Fatal("distinct session lineages must have distinct ResultIDs")
	}
	// 分组键参与标识。
	if ResultID("a", tw) == ResultID("b", tw) {
		t.Fatal("ResultID must include group key")
	}
	// 类型参与标识。
	if ResultID("k", tw) == ResultID("k", s1) {
		t.Fatal("ResultID must include window kind")
	}
}
