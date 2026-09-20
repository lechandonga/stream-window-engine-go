package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lechandonga/stream-window-engine-go/aggregate"
	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/window"
)

// ErrSpillFailed 落盘写入失败（归入检查点/状态写入失败类，调用方应中止处理）。
var ErrSpillFailed = errors.New("spill write failed")

// spillManager 管理窗口状态的本地落盘文件。
// 不变量：
//   - 文件名由窗口 ID 确定性推导，重启后无需额外映射即可定位；
//   - 写入采用 tmp+rename 原子方式，不会留下半写文件；
//   - 检查点登记的 spill 文件与实际文件一一对应；清理仅发生在
//     窗口被彻底关闭且检查点保存成功之后。
type spillManager struct {
	dir        string
	active     map[string]string // windowID -> 文件名（已登记到检查点）
	available  map[string]string // windowID -> 文件名（已写盘但尚未被新检查点登记）
	pendingDel []string          // 已合并移除，待检查点成功后物理删除的文件名
	aggKind    aggregate.Kind
}

func newSpillManager(dir string, aggKind aggregate.Kind, files []string) *spillManager {
	m := &spillManager{
		dir:       dir,
		active:    map[string]string{},
		available: map[string]string{},
		aggKind:   aggKind,
	}
	for _, f := range files {
		m.active[filepath.Base(f)] = f
	}
	return m
}

func (m *spillManager) path(ref string) string { return filepath.Join(m.dir, ref) }

// spillWindow 把窗口状态原子写入 spill 文件，返回引用文件名。
func (m *spillManager) spillWindow(key, id string, st *windowState, wk window.Kind) (string, error) {
	mk := spillMapKey(key, id)
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return "", fmt.Errorf("%w: mkdir: %v", ErrSpillFailed, err)
	}
	data, err := st.agg.Marshal()
	if err != nil {
		return "", fmt.Errorf("%w: marshal: %v", ErrSpillFailed, err)
	}
	seen := make([]string, 0, len(st.seen))
	for eid := range st.seen {
		seen = append(seen, eid)
	}
	sort.Strings(seen)
	dto := windowStateDTO{
		StartMs: st.w.Start.UnixMilli(), EndMs: st.w.End.UnixMilli(),
		AggKind: string(st.agg.Kind()), AggData: string(data),
		Seen: seen, Fired: st.fired, Retracted: st.retracted,
		LastSeq: st.lastSeq, LastValue: st.lastValue,
	}
	raw, err := json.Marshal(dto)
	if err != nil {
		return "", fmt.Errorf("%w: encode: %v", ErrSpillFailed, err)
	}
	ref := spillRefFromWindowID(key+"|"+id, wk)
	final := m.path(ref)
	tmp, err := os.CreateTemp(m.dir, ".spill-*")
	if err != nil {
		return "", fmt.Errorf("%w: create temp: %v", ErrSpillFailed, err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(raw); err != nil {
		return "", fmt.Errorf("%w: write: %v", ErrSpillFailed, err)
	}
	if err := tmp.Sync(); err != nil {
		return "", fmt.Errorf("%w: fsync: %v", ErrSpillFailed, err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("%w: close: %v", ErrSpillFailed, err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		return "", fmt.Errorf("%w: rename: %v", ErrSpillFailed, err)
	}
	cleanup = false
	m.available[mk] = ref
	return ref, nil
}

// loadWindow 从 spill 文件惰性加载窗口状态。
func (m *spillManager) loadWindow(id, ref string, wk window.Kind) (*windowState, error) {
	raw, err := os.ReadFile(m.path(ref))
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", checkpoint.ErrCorrupt, ref, err)
	}
	var dto windowStateDTO
	if err := json.Unmarshal(raw, &dto); err != nil {
		return nil, fmt.Errorf("%w: parse spill %s: %v", checkpoint.ErrCorrupt, ref, err)
	}
	aggKind := aggregate.Kind(dto.AggKind)
	if aggKind == "" {
		aggKind = m.aggKind
	}
	agg, err := aggregate.New(aggKind)
	if err != nil {
		return nil, err
	}
	if err := agg.Unmarshal([]byte(dto.AggData)); err != nil {
		return nil, fmt.Errorf("%w: agg spill %s: %v", checkpoint.ErrCorrupt, ref, err)
	}
	seen := make(map[string]struct{}, len(dto.Seen))
	for _, eid := range dto.Seen {
		seen[eid] = struct{}{}
	}
	return &windowState{
		w:   window.Window{Start: time.UnixMilli(dto.StartMs), End: time.UnixMilli(dto.EndMs)},
		agg: agg, seen: seen, fired: dto.Fired, retracted: dto.Retracted,
		lastSeq: dto.LastSeq, lastValue: dto.LastValue, spilledRef: ref,
	}, nil
}

// commit 检查点保存成功后调用：available 中的 spill 文件转为已登记。
func (m *spillManager) commit() {
	for mk, ref := range m.available {
		m.active[mk] = ref
	}
	m.available = map[string]string{}
}

// markPendingDelete 登记一个待删除的 spill 文件（窗口已被会话合并移除）。
// 物理删除延后到下一次检查点成功，避免“状态已删但检查点还指向它”的半更新。
func (m *spillManager) markPendingDelete(ref string) {
	m.pendingDel = append(m.pendingDel, ref)
}

// applyPendingDeletes 在检查点保存成功后物理删除待删文件。
func (m *spillManager) applyPendingDeletes() error {
	var firstErr error
	for _, ref := range m.pendingDel {
		if err := os.Remove(m.path(ref)); err != nil && !errors.Is(err, os.ErrNotExist) {
			if firstErr == nil {
				firstErr = fmt.Errorf("%w: remove merged spill %s: %v", ErrSpillFailed, ref, err)
			}
		}
	}
	m.pendingDel = nil
	return firstErr
}

// remove 在窗口彻底关闭且检查点成功后删除其 spill 文件。
func (m *spillManager) remove(key, id string) error {
	mk := spillMapKey(key, id)
	ref, ok := m.active[mk]
	if !ok {
		ref, ok = m.available[mk]
	}
	if !ok {
		return nil
	}
	if err := os.Remove(m.path(ref)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: remove %s: %v", ErrSpillFailed, ref, err)
	}
	delete(m.active, mk)
	delete(m.available, mk)
	return nil
}

// reconcileOrphans 删除磁盘上未被任何状态引用的 spill 文件（清理历史遗留，
// 防止无界增长）。在恢复后、首次检查点前调用。返回第一个删除错误。
func (m *spillManager) reconcileOrphans() error {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	referenced := map[string]struct{}{}
	for _, r := range m.active {
		referenced[r] = struct{}{}
	}
	var firstErr error
	for _, en := range entries {
		name := en.Name()
		if !strings.HasPrefix(name, "spill-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		if _, ok := referenced[name]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(m.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			if firstErr == nil {
				firstErr = fmt.Errorf("%w: orphan remove %s: %v", ErrSpillFailed, name, err)
			}
		}
	}
	return firstErr
}

// registeredFiles 返回需要写入下一次检查点的 spill 引用（确定性排序）。
func (m *spillManager) registeredFiles() []string {
	dropping := map[string]struct{}{}
	for _, r := range m.pendingDel {
		dropping[r] = struct{}{}
	}
	refs := make(map[string]struct{}, len(m.active)+len(m.available))
	for _, r := range m.active {
		if _, drop := dropping[r]; drop {
			continue
		}
		refs[r] = struct{}{}
	}
	for _, r := range m.available {
		if _, drop := dropping[r]; drop {
			continue
		}
		refs[r] = struct{}{}
	}
	out := make([]string, 0, len(refs))
	for r := range refs {
		out = append(out, filepath.Join(m.dir, r))
	}
	sort.Strings(out)
	return out
}

// unregister 在窗口惰性加载回内存后注销 spill 文件（文件本身保留到下次
// 检查点成功后删除，避免“引用已删但检查点仍指向它”的窗口）。
func (m *spillManager) unregister(key, id string, wk window.Kind) {
	mk := spillMapKey(key, id)
	ref := spillRefFromWindowID(mk, wk)
	delete(m.active, mk)
	delete(m.available, mk)
	m.pendingDel = append(m.pendingDel, ref)
}

func spillMapKey(key, id string) string { return key + "|" + id }
