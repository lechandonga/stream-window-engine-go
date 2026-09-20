package aggregate

import (
	"fmt"
	"strconv"
)

// Kind 标识聚合函数。
type Kind string

const (
	Count Kind = "count"
	Sum   Kind = "sum"
)

// Aggregator 聚合事件值，支持合并（用于会话窗口合并）与序列化（用于检查点/落盘）。
type Aggregator interface {
	Kind() Kind
	Add(v int64)
	Merge(other Aggregator)
	Value() int64
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

// New 按名称创建聚合器。
func New(kind Kind) (Aggregator, error) {
	switch kind {
	case Count:
		return &countAgg{}, nil
	case Sum:
		return &sumAgg{}, nil
	default:
		return nil, fmt.Errorf("unknown aggregate kind: %q", kind)
	}
}

// countAgg 忽略事件值，按事件条数计数。
type countAgg struct{ n int64 }

func (a *countAgg) Kind() Kind   { return Count }
func (a *countAgg) Add(int64)    { a.n++ }
func (a *countAgg) Value() int64 { return a.n }
func (a *countAgg) Merge(other Aggregator) {
	a.n += other.Value()
}
func (a *countAgg) Marshal() ([]byte, error) {
	return []byte(strconv.FormatInt(a.n, 10)), nil
}
func (a *countAgg) Unmarshal(data []byte) error {
	v, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return fmt.Errorf("unmarshal count: %w", err)
	}
	a.n = v
	return nil
}

// sumAgg 对事件值求和。
type sumAgg struct{ sum int64 }

func (a *sumAgg) Kind() Kind   { return Sum }
func (a *sumAgg) Add(v int64)  { a.sum += v }
func (a *sumAgg) Value() int64 { return a.sum }
func (a *sumAgg) Merge(other Aggregator) {
	a.sum += other.Value()
}
func (a *sumAgg) Marshal() ([]byte, error) {
	return []byte(strconv.FormatInt(a.sum, 10)), nil
}
func (a *sumAgg) Unmarshal(data []byte) error {
	v, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return fmt.Errorf("unmarshal sum: %w", err)
	}
	a.sum = v
	return nil
}
