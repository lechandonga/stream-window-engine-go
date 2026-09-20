// Package sink 定义结果输出：带确认协议的本地持久化输出，
// 已确认输出在重启后不重复发出；重复投递携带幂等键由下游去重。
package sink

import (
	"errors"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
)

// ErrSink 输出侧失败（与源中断/检查点失败区分）。
var ErrSink = errors.New("sink failure")

// SideRecord 是迟到侧输出/异常事件的旁记录。
type SideRecord struct {
	Decision event.Decision
	Event    event.Event
}

// Sink 接收聚合输出与旁路记录。
type Sink interface {
	// Deliver 投递输出；必须幂等：相同 Seq 重复投递不会产生重复结果。
	Deliver(o aggregate.Output) error
	// DeliverSide 记录被特殊处理的事件（迟到丢弃/侧输出/异常时间）。
	DeliverSide(r SideRecord) error
	// LastSeq 返回已确认的最大 Seq（恢复用）。
	LastSeq() int64
	// Outputs 按投递顺序返回已确认输出（测试断言用）。
	Outputs() []aggregate.Output
	// Sides 返回全部旁路记录。
	Sides() []SideRecord
	// Close 关闭输出。
	Close() error
}
