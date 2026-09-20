package sink

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
)

// FileSink 将输出以 JSONL 追加持久化：
//
//	<path>        主输出，每行一条 Output；
//	<path>.side   旁路记录（迟到/异常事件）。
//
// 启动时扫描恢复全部已确认输出与 LastSeq，因此重启后不会重复发出；
// Deliver 以 Seq 为幂等键，即使上游重投相同 Seq 也只落盘一次。
type FileSink struct {
	mu    sync.Mutex
	path  string
	side  string
	out   map[int64]aggregate.Output
	order []int64
	sides []SideRecord
	fh    *os.File
	sfh   *os.File
}

// NewFile 创建/恢复文件输出（path 不存在则创建）。
func NewFile(path string) (*FileSink, error) {
	if path == "" {
		return nil, errors.Join(ErrSink, errors.New("empty sink path"))
	}
	f := &FileSink{
		path: path,
		side: path + ".side",
		out:  map[int64]aggregate.Output{},
	}
	if err := f.recover(); err != nil {
		return nil, err
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, errors.Join(ErrSink, err)
		}
	}
	fh, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, errors.Join(ErrSink, err)
	}
	sfh, err := os.OpenFile(f.side, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fh.Close()
		return nil, errors.Join(ErrSink, err)
	}
	f.fh, f.sfh = fh, sfh
	return f, nil
}

func (f *FileSink) recover() error {
	if err := f.loadLines(f.path, func(b []byte) error {
		var o aggregate.Output
		if err := json.Unmarshal(b, &o); err != nil {
			return err
		}
		if _, ok := f.out[o.Seq]; !ok {
			f.out[o.Seq] = o
			f.order = append(f.order, o.Seq)
		}
		return nil
	}); err != nil {
		return errors.Join(ErrSink, err)
	}
	if err := f.loadLines(f.side, func(b []byte) error {
		var r SideRecord
		if err := json.Unmarshal(b, &r); err != nil {
			return err
		}
		f.sides = append(f.sides, r)
		return nil
	}); err != nil && !os.IsNotExist(err) {
		return errors.Join(ErrSink, err)
	}
	return nil
}

func (f *FileSink) loadLines(path string, fn func([]byte) error) error {
	fh, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer fh.Close()
	r := bufio.NewReader(fh)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			// 容忍末尾无换行；坏行必须报错（不静默吞掉已损坏的输出）。
			if e := fn(line); e != nil {
				return e
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Deliver 幂等追加输出。
func (f *FileSink) Deliver(o aggregate.Output) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.out[o.Seq]; exists {
		return nil
	}
	b, err := json.Marshal(o)
	if err != nil {
		return errors.Join(ErrSink, err)
	}
	b = append(b, '\n')
	if _, err := f.fh.Write(b); err != nil {
		return errors.Join(ErrSink, err)
	}
	if err := f.fh.Sync(); err != nil {
		return errors.Join(ErrSink, err)
	}
	f.out[o.Seq] = o
	f.order = append(f.order, o.Seq)
	return nil
}

// DeliverSide 追加旁路记录。
func (f *FileSink) DeliverSide(r SideRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := json.Marshal(r)
	if err != nil {
		return errors.Join(ErrSink, err)
	}
	b = append(b, '\n')
	if _, err := f.sfh.Write(b); err != nil {
		return errors.Join(ErrSink, err)
	}
	f.sides = append(f.sides, r)
	return nil
}

// LastSeq 返回已确认的最大 Seq。
func (f *FileSink) LastSeq() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var max int64
	for s := range f.out {
		if s > max {
			max = s
		}
	}
	return max
}

// Outputs 按 Seq 升序返回已确认输出。
func (f *FileSink) Outputs() []aggregate.Output {
	f.mu.Lock()
	defer f.mu.Unlock()
	seqs := append([]int64(nil), f.order...)
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	out := make([]aggregate.Output, 0, len(seqs))
	for _, s := range seqs {
		out = append(out, f.out[s])
	}
	return out
}

// Sides 返回全部旁路记录。
func (f *FileSink) Sides() []SideRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]SideRecord(nil), f.sides...)
}

// Close 刷盘并关闭。
func (f *FileSink) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var err error
	if f.fh != nil {
		err = errors.Join(err, f.fh.Close())
	}
	if f.sfh != nil {
		err = errors.Join(err, f.sfh.Close())
	}
	if err != nil {
		return errors.Join(ErrSink, err)
	}
	return nil
}
