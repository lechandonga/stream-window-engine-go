package engine_test

import (
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/checkpoint"
	"github.com/lechandonga/stream-window-engine-go/engine"
	"github.com/lechandonga/stream-window-engine-go/event"
	"github.com/lechandonga/stream-window-engine-go/sink"
)

// 场景：窗口已输出 -> 关闭 -> 新引擎从检查点+持久 sink 恢复 ->
// 迟到事件仍能产生修正，且已确认输出不会重复发出。
func TestRestorePreservesStateAndNoDuplicateOutput(t *testing.T) {
	dir := t.TempDir()
	base := time.Unix(0, 0)

	cfg := baseCfg()
	cfg.AllowedLateness = 60 * time.Second
	cfg.CheckpointDir = dir
	cfg.SpillDir = dir

	sink1, err := sink.NewFileSink(dir + "/out.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	store1 := checkpoint.NewFileStore(dir)
	e1, err := engine.New(cfg, engine.Options{Sink: sink1, Store: store1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e1.ProcessBatch([]event.Event{
		{ID: "a", Key: "k", Timestamp: base.Add(1 * time.Second), Value: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e1.ProcessBatch([]event.Event{
		{ID: "wm1", Key: "k", Timestamp: base.Add(12 * time.Second), Value: 0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e1.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := sink1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：新的引擎、新的 sink 句柄（扫描历史文件重建去重集）。
	sink2, err := sink.NewFileSink(dir + "/out.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	store2 := checkpoint.NewFileStore(dir)
	e2, err := engine.New(cfg, engine.Options{Sink: sink2, Store: store2})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Restore(); err != nil {
		t.Fatal(err)
	}
	if wm := e2.WatermarkMs(); wm != 12000 {
		t.Fatalf("watermark restored=%d want 12000", wm)
	}

	// 重启后到达迟到事件：必须产生修正，不重复首次输出。
	if _, err := e2.ProcessBatch([]event.Event{
		{ID: "late1", Key: "k", Timestamp: base.Add(2 * time.Second), Value: 7},
	}); err != nil {
		t.Fatal(err)
	}
	last, ok := sink2.LastSeq()
	if !ok || last != 2 {
		t.Fatalf("after restart last seq=%v,%v want seq 2", last, ok)
	}
	if err := e2.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := sink2.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次重启：sink 文件里 seq1 只出现一次，且恢复不重发。
	sink3, err := sink.NewFileSink(dir + "/out.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if last, _ := sink3.LastSeq(); last != 2 {
		t.Fatalf("persisted last seq=%d want 2", last)
	}
	if n := countSinkLines(t, dir+"/out.jsonl"); n != 2 {
		t.Fatalf("sink file lines=%d want 2 (no duplicate output)", n)
	}
	_ = sink3
}

func TestUnfinishedWindowsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	base := time.Unix(0, 0)
	cfg := baseCfg()
	cfg.CheckpointDir = dir
	cfg.SpillDir = dir

	store := checkpoint.NewFileStore(dir)
	e, _ := mkEngineWith(t, cfg, sink.NewMemorySink(), store)
	// 窗口 [0,10s) 有数据但水位线未推进，不应丢失。
	if _, err := e.ProcessBatch([]event.Event{
		{ID: "a", Key: "k", Timestamp: base.Add(1 * time.Second), Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	e2, s2 := mkEngineWith(t, cfg, sink.NewMemorySink(), checkpoint.NewFileStore(dir))
	if err := e2.Restore(); err != nil {
		t.Fatal(err)
	}
	if _, err := e2.ProcessBatch([]event.Event{
		{ID: "b", Key: "k", Timestamp: base.Add(12 * time.Second), Value: 0},
	}); err != nil {
		t.Fatal(err)
	}
	recs := s2.Records()
	if len(recs) != 1 || recs[0].Value != 5 {
		t.Fatalf("unfinished window lost: %+v", recs)
	}
}

func TestCorruptCheckpointDetected(t *testing.T) {
	dir := t.TempDir()
	cfg := baseCfg()
	cfg.CheckpointDir = dir
	store := checkpoint.NewFileStore(dir)
	e, _ := mkEngineWith(t, cfg, sink.NewMemorySink(), store)
	if err := e.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// 破坏检查点文件。
	path := dir + "/checkpoint.json"
	if err := appendByteToFile(path, 'x'); err != nil {
		t.Fatal(err)
	}
	e2, _ := mkEngineWith(t, cfg, sink.NewMemorySink(), checkpoint.NewFileStore(dir))
	err := e2.Restore()
	if err == nil {
		t.Fatal("want corrupt error, got nil")
	}
	if !errorsIs(err, checkpoint.ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestCheckpointWriteFailure(t *testing.T) {
	cfg := baseCfg()
	st := checkpoint.NewMemoryStore()
	st.SetSaveError(checkpoint.ErrWriteFailed)
	e, _ := mkEngineWith(t, cfg, sink.NewMemorySink(), st)
	_, err := e.ProcessBatch([]event.Event{
		{ID: "a", Key: "k", Timestamp: time.Unix(1, 0), Value: 1},
	})
	if !errorsIs(err, engine.ErrCheckpointFailed) {
		t.Fatalf("want checkpoint failed, got %v", err)
	}
}
