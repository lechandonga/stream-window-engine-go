package sink

import (
	"bufio"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"sync"
)

// FileSink 本地 append-only JSON Lines 文件输出端。
// 按 Seq 去重（重启后扫描已有文件），每条写入后 fsync，保证崩溃后
// 已确认输出不重复、未确认输出可由检查点 Pending 重投。
type FileSink struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	w       *bufio.Writer
	seen    map[int64]struct{}
	lastSeq int64
	have    bool
}

func NewFileSink(path string) (*FileSink, error) {
	s := &FileSink{path: path, seen: map[int64]struct{}{}}
	if err := s.recover(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	s.w = bufio.NewWriter(f)
	return s, nil
}

// recover 扫描历史文件，重建去重集合与 LastSeq；损坏行归类为失败而非静默忽略。
func (s *FileSink) recover() error {
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		var r Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return &CorruptSinkError{Path: s.path, Line: line, Err: err}
		}
		s.seen[r.Seq] = struct{}{}
		if !s.have || r.Seq > s.lastSeq {
			s.lastSeq = r.Seq
			s.have = true
		}
	}
	return sc.Err()
}

// CorruptSinkError 表示输出文件存在无法解析的记录行。
type CorruptSinkError struct {
	Path string
	Line int
	Err  error
}

func (e *CorruptSinkError) Error() string {
	return "sink file corrupt at " + e.Path + ":" + itoa(e.Line) + ": " + e.Err.Error()
}
func (e *CorruptSinkError) Unwrap() error { return e.Err }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func (s *FileSink) Emit(r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[r.Seq]; ok {
		return nil // 幂等
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err := s.w.Write(data); err != nil {
		return errors.Join(ErrSinkUnavailable, err)
	}
	if err := s.w.WriteByte('\n'); err != nil {
		return errors.Join(ErrSinkUnavailable, err)
	}
	if err := s.w.Flush(); err != nil {
		return errors.Join(ErrSinkUnavailable, err)
	}
	if err := s.f.Sync(); err != nil {
		return errors.Join(ErrSinkUnavailable, err)
	}
	s.seen[r.Seq] = struct{}{}
	if !s.have || r.Seq > s.lastSeq {
		s.lastSeq = r.Seq
		s.have = true
	}
	return nil
}

func (s *FileSink) LastSeq() (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSeq, s.have
}

// Close 刷新并关闭输出文件。
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	return s.f.Close()
}
