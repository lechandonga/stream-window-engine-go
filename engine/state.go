package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/lechandonga/stream-window-engine-go/aggregate"
	"github.com/lechandonga/stream-window-engine-go/window"
)

// windowState 是单个 key 的单个窗口实例状态。
type windowState struct {
	w          window.Window
	agg        aggregate.Aggregator
	seen       map[string]struct{} // 本窗口已计入的事件 ID（重复投递去重）
	values     map[string]int64    // 事件 ID -> 值（sum 差量合并）
	contribs   map[string]int64    // 事件 ID -> 对聚合值的贡献（sum 为值，count 为 1）
	fired      bool                // 是否已首次输出
	retracted  bool                // 会话窗口：该窗口是否已被合并撤销
	lastSeq    int64               // 最近一次输出的 Seq
	lastValue  int64
	spilledRef string // 非空表示状态已落盘（惰性加载）
	dirty      bool   // 迟到修订已计入但尚未在屏障输出
}

func newWindowState(w window.Window, kind aggregate.Kind) (*windowState, error) {
	agg, err := aggregate.New(kind)
	if err != nil {
		return nil, err
	}
	return &windowState{
		w:        w,
		agg:      agg,
		seen:     map[string]struct{}{},
		values:   map[string]int64{},
		contribs: map[string]int64{},
		lastSeq:  -1,
	}, nil
}

// keyState 是一个分组键下的全部窗口（按窗口 ID 索引）。
type keyState struct {
	windows map[string]*windowState
}

func newKeyState() *keyState {
	return &keyState{windows: map[string]*windowState{}}
}

// ---- 检查点序列化格式（engine 内部格式 v1）----

type windowStateDTO struct {
	StartMs    int64            `json:"start_ms"`
	EndMs      int64            `json:"end_ms"`
	AggKind    string           `json:"agg_kind"`
	AggData    string           `json:"agg_data"`
	Seen       []string         `json:"seen"`
	Values     map[string]int64 `json:"values,omitempty"`
	Contribs   map[string]int64 `json:"contribs,omitempty"`
	Fired      bool             `json:"fired"`
	Retracted  bool             `json:"retracted"`
	LastSeq    int64            `json:"last_seq"`
	LastValue  int64            `json:"last_value"`
	SpilledRef string           `json:"spilled_ref,omitempty"`
}

type keyStateDTO struct {
	Windows []windowStateDTO `json:"windows"`
}

func (ks *keyState) marshal(kind aggregate.Kind, wk window.Kind) (json.RawMessage, error) {
	dto := keyStateDTO{Windows: make([]windowStateDTO, 0, len(ks.windows))}
	for id, st := range ks.windows {
		if st.spilledRef != "" {
			dto.Windows = append(dto.Windows, windowStateDTO{
				StartMs: st.w.Start.UnixMilli(), EndMs: st.w.End.UnixMilli(),
				LastSeq: st.lastSeq, LastValue: st.lastValue,
				SpilledRef: st.spilledRef, Fired: st.fired,
			})
			continue // 落盘窗口只保存引用与边界，内容在 spill 文件中
		}
		data, err := st.agg.Marshal()
		if err != nil {
			return nil, fmt.Errorf("marshal agg window %s: %w", id, err)
		}
		seen := make([]string, 0, len(st.seen))
		for id := range st.seen {
			seen = append(seen, id)
		}
		sort.Strings(seen)
		vals := map[string]int64{}
		if len(st.values) > 0 {
			vals = st.values
		}
		contribs := map[string]int64{}
		if len(st.contribs) > 0 {
			contribs = st.contribs
		}
		dto.Windows = append(dto.Windows, windowStateDTO{
			StartMs: st.w.Start.UnixMilli(), EndMs: st.w.End.UnixMilli(),
			AggKind: string(st.agg.Kind()), AggData: string(data),
			Seen: seen, Values: vals, Contribs: contribs, Fired: st.fired, Retracted: st.retracted,
			LastSeq: st.lastSeq, LastValue: st.lastValue, SpilledRef: st.spilledRef,
		})
	}
	sort.Slice(dto.Windows, func(i, j int) bool { return dto.Windows[i].StartMs < dto.Windows[j].StartMs })
	return json.Marshal(dto)
}

func unmarshalKeyState(raw json.RawMessage, kind aggregate.Kind, wk window.Kind) (*keyState, error) {
	var dto keyStateDTO
	if err := json.Unmarshal(raw, &dto); err != nil {
		return nil, fmt.Errorf("unmarshal key state: %w", err)
	}
	ks := newKeyState()
	for _, d := range dto.Windows {
		w0 := window.Window{Start: time.UnixMilli(d.StartMs), End: time.UnixMilli(d.EndMs)}
		id := windowID(w0, wk)
		seen := make(map[string]struct{}, len(d.Seen))
		for _, id := range d.Seen {
			seen[id] = struct{}{}
		}
		values := d.Values
		if values == nil {
			values = map[string]int64{}
		}
		contribs := d.Contribs
		if contribs == nil {
			contribs = map[string]int64{}
		}
		if d.SpilledRef != "" {
			ks.windows[id] = &windowState{
				w: w0, agg: mustNewAgg(kind), seen: map[string]struct{}{}, values: map[string]int64{},
				fired: d.Fired, lastSeq: d.LastSeq, lastValue: d.LastValue, spilledRef: d.SpilledRef,
			}
			continue
		}
		aggKind := aggregate.Kind(d.AggKind)
		if aggKind == "" {
			aggKind = kind
		}
		agg, err := aggregate.New(aggKind)
		if err != nil {
			return nil, err
		}
		if err := agg.Unmarshal([]byte(d.AggData)); err != nil {
			return nil, err
		}
		ks.windows[id] = &windowState{
			w: w0, agg: agg, seen: seen, values: values, contribs: contribs,
			fired: d.Fired, retracted: d.Retracted,
			lastSeq: d.LastSeq, lastValue: d.LastValue,
		}
	}
	return ks, nil
}

func mustNewAgg(kind aggregate.Kind) aggregate.Aggregator {
	a, err := aggregate.New(kind)
	if err != nil {
		panic(err)
	}
	return a
}
