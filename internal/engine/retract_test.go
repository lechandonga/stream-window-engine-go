package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/source"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

// sessionCfg 会话窗口配置：gap=5s，允许迟到宽限 60s，逐条屏障（CheckpointEvery=1）。
func sessionCfg(late event.LatePolicy) Config {
	return Config{
		Window:          window.Spec{Kind: window.Session, Gap: 5 * time.Second},
		Aggregator:      "sum_count",
		Lag:             0,
		Late:            late,
		AllowedLateness: 60 * time.Second,
		Invalid:         event.InvalidReject,
		FutureBound:     24 * time.Hour,
		State:           state.Config{},
		CheckpointEvery: 1,
	}
}

// view 模拟下游「分组+窗口」增量视图：按 Seq 幂等应用输出序列。
type view struct {
	applied map[int64]bool
	rows    map[string]aggregate.Output // ResultID -> 当前生效结果
}

func newView() *view { return &view{applied: map[int64]bool{}, rows: map[string]aggregate.Output{}} }

func (v *view) apply(o aggregate.Output) {
	if v.applied[o.Seq] {
		return // 重复投递/重放：幂等跳过
	}
	v.applied[o.Seq] = true
	if o.Kind == aggregate.Retract {
		delete(v.rows, o.ResultID) // 已撤销过则不存在，删除是 no-op
		return
	}
	v.rows[o.ResultID] = o
}

// 迟到桥接事件把两个已发布的会话并成一个：
// 下游必须先收到两条 RETRACT（指明被撤标识与替代者），再收到合并结果，
// 最终视图中不存在幽灵记录。
func TestSessionMergeRetractsPublishedResults(t *testing.T) {
	cfg := sessionCfg(event.LateInclude)
	events := []event.Event{
		eid("e1", "k", 0, 1),     // 会话 S1=[0,5)
		eid("e2", "k", 9, 2),     // 会话 S2=[9,14)（间隔 9>5，独立）
		eid("adv", "k", 40, 0),   // 水位线到 40：S1、S2 先后发布
		eid("bridge", "k", 4, 4), // 迟到桥接：[4,9) 与两侧间隔均 <=5，把 S1、S2 并成 [0,14)
	}
	e := quietEngine(t, cfg)
	dst := sink.NewMemory()
	if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
		t.Fatal(err)
	}
	out := dst.Outputs()

	var kinds []aggregate.OutputKind
	for _, o := range out {
		kinds = append(kinds, o.Kind)
	}
	want := []aggregate.OutputKind{
		aggregate.Insert, aggregate.Insert, // S1、S2 首次发布
		aggregate.Retract, aggregate.Retract, // 撤销 S1、S2
		aggregate.Insert, // 合并后的 [0,14)
		aggregate.Insert, // adv 自身的会话 [40,45)
	}
	if len(kinds) != len(want) {
		t.Fatalf("outputs=%d want %d: %+v", len(kinds), len(want), out)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("output %d kind=%s want %s\nall=%+v", i, kinds[i], want[i], out)
		}
	}

	s1, s2 := out[0], out[1]
	r1, r2 := out[2], out[3]
	merged := out[4]

	// 撤销必须精确指向已发布结果的稳定标识。
	if r1.ResultID != s1.ResultID || r2.ResultID != s2.ResultID {
		t.Fatalf("retract ResultID mismatch:\n s1=%s r1=%s\n s2=%s r2=%s",
			s1.ResultID, r1.ResultID, s2.ResultID, r2.ResultID)
	}
	// 撤销携带被撤销时的修订号与聚合值，且指向同一个替代结果。
	if r1.Revision != 1 || r1.Value != 1 || r1.Count != 1 {
		t.Fatalf("retract s1 wrong: %+v", r1)
	}
	if r2.Revision != 1 || r2.Value != 2 || r2.Count != 1 {
		t.Fatalf("retract s2 wrong: %+v", r2)
	}
	if r1.ReplacedBy != merged.ResultID || r2.ReplacedBy != merged.ResultID {
		t.Fatalf("replaced_by must point at merged result:\n r1=%s r2=%s merged=%s",
			r1.ReplacedBy, r2.ReplacedBy, merged.ResultID)
	}
	// 合并结果：全新标识 INSERT rev1，内容齐全。
	if merged.Revision != 1 || merged.Value != 7 || merged.Count != 3 {
		t.Fatalf("merged wrong: %+v", merged)
	}
	if !merged.Window.Start.Equal(testBase) || !merged.Window.End.Equal(testBase.Add(14*time.Second)) {
		t.Fatalf("merged window=[%v,%v) want [0,14)", merged.Window.Start, merged.Window.End)
	}

	// 下游对账：按 Seq 顺序应用后，视图只剩合并会话与 adv 会话，无幽灵。
	v := newView()
	for _, o := range out {
		v.apply(o)
	}
	if len(v.rows) != 2 {
		t.Fatalf("view rows=%d want 2: %+v", len(v.rows), v.rows)
	}
	if _, ok := v.rows[s1.ResultID]; ok {
		t.Fatal("ghost: retracted s1 still in view")
	}
	if _, ok := v.rows[s2.ResultID]; ok {
		t.Fatal("ghost: retracted s2 still in view")
	}
	if got := v.rows[merged.ResultID].Value; got != 7 {
		t.Fatalf("merged value in view=%d want 7", got)
	}

	// 幂等消费：整个输出序列再重放一遍，视图不变。
	for _, o := range out {
		v.apply(o)
	}
	if len(v.rows) != 2 || v.rows[merged.ResultID].Value != 7 {
		t.Fatalf("replay changed view: %+v", v.rows)
	}
	if e.Stats().Retractions != 2 {
		t.Fatalf("retractions stat=%d want 2", e.Stats().Retractions)
	}
}

// 不同并发度（1/4/8）下，含撤销的完整输出序列必须逐条一致：
// Seq、Kind、窗口标识（含确定性会话序号）、撤销指向均不允许漂移。
func TestSessionRetractSameAcrossConcurrency(t *testing.T) {
	var events []event.Event
	groups := []string{"g1", "g2", "g3", "g4"}
	// 每个分组 4 条进入第一个屏障（共 16 条 = barrierSize）：
	// 两个独立会话先发布，两个水位线推进事件（自身会话未触发）。
	for _, g := range groups {
		events = append(events,
			eid(g+"-e1", g, 0, 1),
			eid(g+"-e2", g, 9, 2),
			eid(g+"-adv1", g, 40, 0),
			eid(g+"-adv2", g, 41, 0),
		)
	}
	// 第二个屏障（收尾残余批次）：每组一条迟到桥接事件（与两侧间隔均
	// <=5s），触发撤销+替代。
	for _, g := range groups {
		events = append(events, eid(g+"-bridge", g, 4, 4))
	}
	run := func(conc int) []aggregate.Output {
		cfg := sessionCfg(event.LateInclude)
		cfg.AllowedLateness = 400 * time.Second // 保持已发布会话在桥接时仍可修正
		cfg.CheckpointEvery = 16
		cfg.Concurrency = conc
		e := quietEngine(t, cfg)
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		return dst.Outputs()
	}
	serial := run(1)
	for _, conc := range []int{2, 4, 8} {
		got := run(conc)
		if len(got) != len(serial) {
			t.Fatalf("conc=%d outputs=%d want %d", conc, len(got), len(serial))
		}
		for i := range serial {
			a, b := serial[i], got[i]
			if a.Seq != b.Seq || a.Kind != b.Kind || a.Key != b.Key ||
				!a.Window.Start.Equal(b.Window.Start) || !a.Window.End.Equal(b.Window.End) ||
				a.Window.Seq != b.Window.Seq || a.Value != b.Value || a.Count != b.Count ||
				a.Revision != b.Revision || a.ResultID != b.ResultID ||
				a.ReplacedBy != b.ReplacedBy {
				t.Fatalf("conc=%d output %d differs:\n serial=%+v\n got=%+v", conc, i, a, b)
			}
		}
	}
	var retracts, inserts int
	for _, o := range serial {
		switch o.Kind {
		case aggregate.Retract:
			retracts++
		case aggregate.Insert:
			inserts++
		}
	}
	if retracts != 8 { // 4 组 × 2 个被撤会话
		t.Fatalf("retracts=%d want 8", retracts)
	}
	if inserts != 16 { // 8 旧会话 + 4 合并 + 4 个推进事件合并会话
		t.Fatalf("inserts=%d want 16", inserts)
	}
}

// 中断后续跑 + 全新引擎重放：撤销不重复也不遗漏，完整序列与一次跑完一致。
func TestSessionRetractSurvivesRestartAndReplay(t *testing.T) {
	events := []event.Event{
		eid("e1", "k", 0, 1),
		eid("e2", "k", 9, 2),
		eid("adv", "k", 40, 0),
		eid("bridge", "k", 4, 4),
	}
	dir := t.TempDir()
	cfg := func(dir string) Config {
		c := sessionCfg(event.LateInclude)
		c.CheckpointDir = dir
		return c
	}

	// 参考：一次跑完（独立检查点目录，避免与被测运行互相恢复）。
	ref := quietEngine(t, cfg(filepath.Join(dir, "ref")))
	refDst := sink.NewMemory()
	if err := ref.Run(context.Background(), source.NewMemory(events), refDst); err != nil {
		t.Fatal(err)
	}
	refOut := refDst.Outputs()

	// 第一次：前 3 条（两个会话已发布）后中断。
	runDir := filepath.Join(dir, "run")
	e1 := quietEngine(t, cfg(runDir))
	dst1 := sink.NewMemory()
	if err := e1.Run(context.Background(), source.NewMemoryWithFailure(events, 3), dst1); !errors.Is(err, source.ErrInterrupted) {
		t.Fatalf("want ErrInterrupted, got %v", err)
	}
	if n := len(dst1.Outputs()); n != 2 {
		t.Fatalf("outputs before interrupt=%d want 2", n)
	}

	// 第二次：全新引擎 + 同一检查点恢复，sink 预置已确认输出。
	e2 := quietEngine(t, cfg(runDir))
	dst2 := sink.NewMemory()
	for _, o := range dst1.Outputs() {
		if err := dst2.Deliver(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := e2.Run(context.Background(), source.NewMemory(events), dst2); err != nil {
		t.Fatalf("resume: %v", err)
	}
	got := dst2.Outputs()
	if len(got) != len(refOut) {
		t.Fatalf("after restart outputs=%d want %d", len(got), len(refOut))
	}
	for i := range refOut {
		a, b := refOut[i], got[i]
		if a.Seq != b.Seq || a.Kind != b.Kind || a.Key != b.Key ||
			a.ResultID != b.ResultID || a.ReplacedBy != b.ReplacedBy ||
			a.Value != b.Value || a.Count != b.Count || a.Revision != b.Revision ||
			!a.Window.Start.Equal(b.Window.Start) || !a.Window.End.Equal(b.Window.End) ||
			a.Window.Seq != b.Window.Seq {
			t.Fatalf("restart output %d differs:\n ref=%+v\n got=%+v", i, a, b)
		}
	}

	// 第三次：全新引擎再次重放全部输入（sink 复活检查点 + 已含全部确认输出）：
	// 撤销不得重复生效，最终视图无幽灵、无复活。
	e3 := quietEngine(t, cfg(runDir))
	dst3 := sink.NewMemory()
	for _, o := range refOut {
		if err := dst3.Deliver(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := e3.Run(context.Background(), source.NewMemory(events), dst3); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(dst3.Outputs()) != len(refOut) {
		t.Fatalf("replay duplicated outputs: %d vs %d", len(dst3.Outputs()), len(refOut))
	}
	vRef, v3 := newView(), newView()
	for _, o := range refOut {
		vRef.apply(o)
	}
	for _, o := range dst3.Outputs() {
		v3.apply(o)
	}
	if len(v3.rows) != len(vRef.rows) {
		t.Fatalf("view after replay rows=%d want %d", len(v3.rows), len(vRef.rows))
	}
	for id, ro := range vRef.rows {
		go3, ok := v3.rows[id]
		if !ok || go3.Value != ro.Value || go3.Count != ro.Count {
			t.Fatalf("view row %s after replay=%+v want %+v", id, go3, ro)
		}
	}
}

// 会话合并边界（验收专项）：相邻间隔恰好等于 Gap 必须合并成一个会话；
// 超过 Gap（哪怕 1ns）必须是两个独立会话。
func TestSessionGapBoundary(t *testing.T) {
	gapNanos := int64((5 * time.Second) / time.Nanosecond)
	run := func(deltaNanos int64) []aggregate.Output {
		cfg := sessionCfg(event.LateInclude)
		cfg.CheckpointEvery = 3
		e := quietEngine(t, cfg)
		events := []event.Event{
			eid("e1", "k", 0, 1),
			{ID: "e2", Key: "k", Ts: testBase.Add(time.Duration(deltaNanos)), Value: 2},
			eid("adv", "k", 100, 0),
		}
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		return dst.Outputs()
	}
	// 事件 0s 与 5s：间隔 5s 恰好 == Gap=5s → 合并为 [0,10)，仅一个数据会话。
	equal := run(gapNanos)
	var dataSessions []aggregate.Output
	for _, o := range equal {
		// 只统计早于水位线推进事件自身会话（ts=100）的数据窗口。
		if o.Kind != aggregate.Retract && o.Window.Start.Before(testBase.Add(20*time.Second)) {
			dataSessions = append(dataSessions, o)
		}
	}
	if len(dataSessions) != 1 {
		t.Fatalf("gap exactly: want 1 merged data session, got %d (%+v)", len(dataSessions), equal)
	}
	if !dataSessions[0].Window.End.Equal(testBase.Add(10*time.Second)) ||
		dataSessions[0].Value != 3 || dataSessions[0].Count != 2 {
		t.Fatalf("merged boundary session wrong: %+v", dataSessions[0])
	}

	// 事件 0s 与 5s+1ns：间隔只超过 1ns → 必须是两个独立会话。
	over := run(gapNanos + 1)
	dataSessions = dataSessions[:0]
	for _, o := range over {
		if o.Kind != aggregate.Retract && o.Window.Start.Before(testBase.Add(20*time.Second)) {
			dataSessions = append(dataSessions, o)
		}
	}
	if len(dataSessions) != 2 {
		t.Fatalf("gap exceeded: want 2 separate sessions, got %d (%+v)", len(dataSessions), over)
	}
	for _, o := range over {
		if o.Kind == aggregate.Retract {
			t.Fatalf("gap exceeded: no retraction expected, got %+v", o)
		}
	}
	// 两个会话各自只含自己的事件：不能把独立会话的事件算进来。
	first, second := dataSessions[0], dataSessions[1]
	if !first.Window.Start.Equal(testBase) || !first.Window.End.Equal(testBase.Add(5*time.Second)) ||
		first.Value != 1 || first.Count != 1 {
		t.Fatalf("first independent session wrong: %+v", first)
	}
	if !second.Window.Start.Equal(testBase.Add(5*time.Second+time.Nanosecond)) ||
		!second.Window.End.Equal(testBase.Add(10*time.Second+time.Nanosecond)) ||
		second.Value != 2 || second.Count != 1 {
		t.Fatalf("second independent session wrong: %+v", second)
	}

	// 三事件链：0s、5s（恰好上限）必须并入同会话；30s 间隔 25s 独立。
	chain := func() []aggregate.Output {
		cfg := sessionCfg(event.LateInclude)
		cfg.CheckpointEvery = 4
		e := quietEngine(t, cfg)
		events := []event.Event{
			eid("c1", "k", 0, 1),
			eid("c2", "k", 5, 2),
			eid("c3", "k", 30, 3),
			eid("adv", "k", 100, 0),
		}
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		return dst.Outputs()
	}()
	dataSessions = dataSessions[:0]
	for _, o := range chain {
		if o.Kind != aggregate.Retract && o.Window.Start.Before(testBase.Add(60*time.Second)) {
			dataSessions = append(dataSessions, o)
		}
	}
	if len(dataSessions) != 2 {
		t.Fatalf("chain: want [0,10) and [30,35), got %d: %+v", len(dataSessions), chain)
	}
	if !dataSessions[0].Window.End.Equal(testBase.Add(10*time.Second)) ||
		dataSessions[0].Value != 3 || dataSessions[0].Count != 2 {
		t.Fatalf("chain merged session wrong: %+v", dataSessions[0])
	}
	if !dataSessions[1].Window.Start.Equal(testBase.Add(30*time.Second)) ||
		!dataSessions[1].Window.End.Equal(testBase.Add(35*time.Second)) ||
		dataSessions[1].Value != 3 || dataSessions[1].Count != 1 {
		t.Fatalf("chain independent session wrong: %+v", dataSessions[1])
	}
}

// 迟到事件落在已发布会话区间内（合并后窗口标识不变）：
// 走 CORRECTION，不产生撤销，不新增幽灵结果。
func TestSessionExtensionSameIdentityCorrects(t *testing.T) {
	cfg := sessionCfg(event.LateInclude)
	events := []event.Event{
		eid("e1", "k", 0, 1), // [0,5)
		eid("e2", "k", 5, 2), // [5,10)，与 [0,5) 首尾相接（间隔恰好 5）→ 合并 [0,10)
		eid("adv", "k", 40, 0),
		eid("e3", "k", 3, 4), // 迟到，[3,8) 并入后窗口仍是 [0,10)
	}
	e := quietEngine(t, cfg)
	dst := sink.NewMemory()
	if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
		t.Fatal(err)
	}
	out := dst.Outputs()
	var inserts, corrections, retracts int
	var corrected aggregate.Output
	for _, o := range out {
		switch o.Kind {
		case aggregate.Insert:
			inserts++
		case aggregate.Correction:
			corrections++
			corrected = o
		case aggregate.Retract:
			retracts++
		}
	}
	if retracts != 0 {
		t.Fatalf("identity-preserved extension must not retract, got %d: %+v", retracts, out)
	}
	if corrections != 1 {
		t.Fatalf("want exactly 1 CORRECTION, got %d: %+v", corrections, out)
	}
	if !corrected.Window.Start.Equal(testBase) || !corrected.Window.End.Equal(testBase.Add(10*time.Second)) ||
		corrected.Value != 7 || corrected.Count != 3 || corrected.Revision != 2 {
		t.Fatalf("correction wrong: %+v", corrected)
	}
	// [0,15) 首次插入存在（rev1）。
	found := false
	for _, o := range out {
		if o.Kind == aggregate.Insert && o.Window.End.Equal(testBase.Add(10*time.Second)) && o.Revision == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing INSERT rev1 for [0,10): %+v", out)
	}
}

// 会话窗口重复投递（含迟到的二次投递与重启重放）：按事件 ID 去重，
// 不双倍计数；重复事件本身不产生任何输出变化。
// 真正的迟到新事件 b 把已发布的 [0,5) 扩展为新标识 [0,7)：
// 此时应是 撤销[0,5) + INSERT[0,7)，且计数/值不被重复事件污染。
func TestSessionDuplicateRedelivery(t *testing.T) {
	cfg := sessionCfg(event.LateInclude)
	events := []event.Event{
		eid("a", "k", 0, 10),
		eid("adv", "k", 40, 0), // 发布 [0,5)
		eid("a", "k", 0, 10),   // 迟到的重复投递：去重，无输出变化
		eid("b", "k", 2, 3),    // 真正的迟到新事件：[2,7) 与 [0,5) 并成 [0,7)
	}
	e := quietEngine(t, cfg)
	dst := sink.NewMemory()
	if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
		t.Fatal(err)
	}
	out := dst.Outputs()
	var inserts, retracts int
	var replaced aggregate.Output
	for _, o := range out {
		switch o.Kind {
		case aggregate.Insert:
			inserts++
			if o.Window.End.Equal(testBase.Add(7 * time.Second)) {
				replaced = o
			}
		case aggregate.Retract:
			retracts++
		}
	}
	if retracts != 1 {
		t.Fatalf("want exactly 1 RETRACT (only b changed the identity), got %d: %+v", retracts, out)
	}
	if replaced.Value != 13 || replaced.Count != 2 {
		t.Fatalf("duplicate double-counted: %+v", replaced)
	}
	if e.Stats().Duplicates != 1 {
		t.Fatalf("duplicates stat=%d want 1", e.Stats().Duplicates)
	}
}

// 打开内存上限（落盘或背压）时，已发布会话被迟到桥接合并：
// 撤销不丢、不重，替代结果内容正确。
func TestSessionRetractUnderMemoryLimit(t *testing.T) {
	events := []event.Event{
		eid("e1", "k", 0, 1),
		eid("e2", "k", 9, 2),
		eid("adv", "k", 40, 0),
		eid("bridge", "k", 4, 4),
	}
	for _, policy := range []state.OverflowPolicy{state.OverflowSpill, state.OverflowBlock} {
		t.Run(policyName(policy), func(t *testing.T) {
			base := t.TempDir()
			cfg := sessionCfg(event.LateInclude)
			// 桥接事件写入时瞬时存在 4 个会话键（S1、S2、adv、桥接初始窗）。
			// 落盘模式给 cap=3 强制触发淘汰读回；背压模式给 cap=4——
			// 长宽限下窗口未到 purge 条件，背压会忠实等待（符合其语义）。
			cap := 3
			if policy == state.OverflowBlock {
				cap = 4
			}
			cfg.State = state.Config{MaxActiveKeys: cap, Overflow: policy, SpillDir: filepath.Join(base, "spill")}
			cfg.CheckpointDir = filepath.Join(base, "chk")
			e := quietEngine(t, cfg)
			dst := sink.NewMemory()
			if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
				t.Fatal(err)
			}
			out := dst.Outputs()
			var retracts int
			var merged aggregate.Output
			for _, o := range out {
				if o.Kind == aggregate.Retract {
					retracts++
				}
				if o.Kind == aggregate.Insert && o.Window.End.Equal(testBase.Add(14*time.Second)) {
					merged = o
				}
			}
			if retracts != 2 {
				t.Fatalf("policy=%s retracts=%d want 2: %+v", policyName(policy), retracts, out)
			}
			if merged.Value != 7 || merged.Count != 3 {
				t.Fatalf("policy=%s merged wrong: %+v", policyName(policy), merged)
			}
			// 下游对账：旧会话标识不在视图中。
			v := newView()
			for _, o := range out {
				v.apply(o)
			}
			for _, o := range out {
				if o.Kind == aggregate.Retract {
					if _, ok := v.rows[o.ResultID]; ok {
						t.Fatalf("ghost after retraction under %s: %s", policyName(policy), o.ResultID)
					}
				}
			}
		})
	}
}

// 落盘 + 中断恢复：桥接前中断，恢复后（部分会话条目可能在磁盘上）
// 撤销仍完整且与一次跑完一致。
func TestSessionRetractWithSpillAcrossRestart(t *testing.T) {
	base := t.TempDir()
	events := []event.Event{
		eid("e1", "k", 0, 1),
		eid("e2", "k", 9, 2),
		eid("adv", "k", 40, 0),
		eid("bridge", "k", 4, 4),
	}
	cfg := func() Config {
		c := sessionCfg(event.LateInclude)
		c.State = state.Config{
			MaxActiveKeys: 2,
			Overflow:      state.OverflowSpill,
			SpillDir:      filepath.Join(base, "spill"),
		}
		c.CheckpointDir = filepath.Join(base, "chk")
		return c
	}
	e1 := quietEngine(t, cfg())
	dst1 := sink.NewMemory()
	if err := e1.Run(context.Background(), source.NewMemoryWithFailure(events, 3), dst1); !errors.Is(err, source.ErrInterrupted) {
		t.Fatalf("want ErrInterrupted, got %v", err)
	}
	e2 := quietEngine(t, cfg())
	dst2 := sink.NewMemory()
	for _, o := range dst1.Outputs() {
		if err := dst2.Deliver(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := e2.Run(context.Background(), source.NewMemory(events), dst2); err != nil {
		t.Fatalf("resume: %v", err)
	}
	out := dst2.Outputs()
	var retracts int
	var merged aggregate.Output
	for _, o := range out {
		if o.Kind == aggregate.Retract {
			retracts++
		}
		if o.Kind == aggregate.Insert && o.Window.End.Equal(testBase.Add(14*time.Second)) {
			merged = o
		}
	}
	if retracts != 2 {
		t.Fatalf("retracts after spill+restart=%d want 2: %+v", retracts, out)
	}
	if merged.Value != 7 || merged.Count != 3 {
		t.Fatalf("merged after spill+restart wrong: %+v", merged)
	}
	seenSeq := map[int64]bool{}
	for _, o := range out {
		if seenSeq[o.Seq] {
			t.Fatalf("duplicate seq %d after spill+restart", o.Seq)
		}
		seenSeq[o.Seq] = true
	}
}

func policyName(p state.OverflowPolicy) string {
	if p == state.OverflowSpill {
		return "spill"
	}
	return "block"
}

// 验收：前一个会话已经发布过结果，然后来一条超过上限的迟到事件。
// 已发布结果依然成立：不得对它发撤销；迟到事件按自己的会话正常发布
// 一份结果，下游视图里两个会话各占一行（无幽灵、不丢有效行）。
func TestSessionLateEventBeyondGapKeepsPublishedRow(t *testing.T) {
	gap := 5 * time.Second
	run := func(lateTsSec int64, lateTsNanoseconds int) ([]aggregate.Output, *Engine) {
		cfg := sessionCfg(event.LateInclude)
		cfg.Window.Gap = gap
		cfg.CheckpointEvery = 3
		e := quietEngine(t, cfg)
		events := []event.Event{
			eid("e1", "k", 0, 1),
			eid("adv", "k", 20, 0), // 发布 [0,5)（水位线 20，迟到仍在宽限期内）
			{ID: "late", Key: "k",
				Ts:    testBase.Add(time.Duration(lateTsSec)*time.Second + time.Duration(lateTsNanoseconds)),
				Value: 9},
		}
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		return dst.Outputs(), e
	}

	// 刚超过上限 1ns（迟到首事件 5s+1ns）：两个独立会话，零撤销。
	out, eng := run(5, 1)
	var kinds []aggregate.OutputKind
	for _, o := range out {
		kinds = append(kinds, o.Kind)
	}
	want := []aggregate.OutputKind{
		aggregate.Insert, // 已发布 [0,5)
		aggregate.Insert, // adv 会话 [20,25)
		aggregate.Insert, // 迟到事件自己的会话（5.000000001 起）
	}
	if len(kinds) != len(want) {
		t.Fatalf("outputs=%d want %d: %+v", len(kinds), len(want), out)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("output %d kind=%s want %s: %+v", i, kinds[i], want[i], out)
		}
	}
	for _, o := range out {
		if o.Kind == aggregate.Retract {
			t.Fatalf("published result beyond gap must not be retracted: %+v", o)
		}
	}
	if eng.Stats().Retractions != 0 {
		t.Fatalf("retractions stat=%d want 0", eng.Stats().Retractions)
	}
	published := out[0]
	if !published.Window.Start.Equal(testBase) || !published.Window.End.Equal(testBase.Add(gap)) ||
		published.Value != 1 || published.Count != 1 {
		t.Fatalf("published session must stay unchanged: %+v", published)
	}
	// 输出按窗口起点排序：迟到会话 [5.000000001,…) 在 [0,5) 之后、adv 之前。
	lateSess := out[1]
	lateStart := testBase.Add(gap + time.Nanosecond)
	if !lateSess.Window.Start.Equal(lateStart) || !lateSess.Window.End.Equal(lateStart.Add(gap)) ||
		lateSess.Value != 9 || lateSess.Count != 1 {
		t.Fatalf("late event must publish its own clean session: %+v", lateSess)
	}

	// 下游对账：视图三行（[0,5)、迟到会话、adv 会话），已发布行原封不动。
	v := newView()
	for _, o := range out {
		v.apply(o)
	}
	if len(v.rows) != 3 {
		t.Fatalf("view rows=%d want 3: %+v", len(v.rows), v.rows)
	}
	if row, ok := v.rows[published.ResultID]; !ok || row.Value != 1 || row.Count != 1 {
		t.Fatalf("valid published row lost or altered: ok=%v row=%+v", ok, row)
	}
	if row := v.rows[lateSess.ResultID]; row.Value != 9 || row.Count != 1 {
		t.Fatalf("late session row wrong: %+v", row)
	}

	// 间隔恰好等于上限（迟到首事件 5s）：迟到事件必须并入已发布会话，
	// 不产生撤销、不新增独立行；（逐事件屏障下合并发生在首次发布前，
	// 因此以单一合并会话 [0,10) 的 INSERT 呈现；延迟发布时则为
	// CORRECTION，两种呈现都不允许出现撤销——详见 TestSessionExtensionSameIdentityCorrects。）
	equal, _ := run(5, 0)
	var retracts int
	var data []aggregate.Output
	for _, o := range equal {
		if o.Kind == aggregate.Retract {
			retracts++
		}
		if o.Window.Start.Before(testBase.Add(10 * time.Second)) {
			data = append(data, o)
		}
	}
	if retracts != 0 {
		t.Fatalf("gap exactly equal must never retract, got %d: %+v", retracts, equal)
	}
	if len(data) != 1 || !data[0].Window.End.Equal(testBase.Add(10*time.Second)) ||
		data[0].Value != 10 || data[0].Count != 2 {
		t.Fatalf("gap exactly equal must yield one merged session [0,10) sum=10 count=2, got %+v", equal)
	}
}

// 多轮合并：两条迟到桥接分两轮把三个已发布会话串成一个，每一轮都走
// “先撤销旧标识、再发布新标识”，最终替代结果只包含本会话的事件；
// 两条桥接以任意顺序到达，撤销总数与最终视图一致（与到达顺序无关）。
func TestSessionMultiRoundMergeOrderIndependent(t *testing.T) {
	eventsForward := []event.Event{
		eid("e1", "k", 0, 1),
		eid("e2", "k", 9, 2),
		eid("e3", "k", 18, 3),
		eid("adv", "k", 60, 0), // 三个独立会话先后发布
		eid("adv2", "k", 61, 0),
		eid("b1", "k", 4, 4), // 第一轮：桥接 e1-e2（两侧间隔均 4）
		eid("adv3", "k", 62, 0),
		eid("b2", "k", 13, 5), // 第二轮：桥接到 e3（18-13=5，恰好上限）
	}
	eventsReverse := []event.Event{
		eventsForward[0], eventsForward[1], eventsForward[2], eventsForward[3], eventsForward[4],
		eventsForward[7], // b2 先到
		eventsForward[6], // adv3
		eventsForward[5], // b1 后到
	}
	run := func(evs []event.Event) []aggregate.Output {
		cfg := sessionCfg(event.LateInclude)
		cfg.CheckpointEvery = 1
		e := quietEngine(t, cfg)
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(evs), dst); err != nil {
			t.Fatal(err)
		}
		return dst.Outputs()
	}
	fwd := run(eventsForward)
	rev := run(eventsReverse)

	// 最终视图：只剩合并会话 [0,23)（sum=15,count=5）与 adv 会话 [60,65)。
	finalView := func(out []aggregate.Output) map[string]aggregate.Output {
		v := newView()
		for _, o := range out {
			v.apply(o)
		}
		return v.rows
	}
	vf, vr := finalView(fwd), finalView(rev)
	if len(vf) != 2 || len(vr) != 2 {
		t.Fatalf("final rows fwd=%d rev=%d", len(vf), len(vr))
	}
	var merged aggregate.Output
	for _, o := range vf {
		if o.Window.Start.Equal(testBase) {
			merged = o
		}
	}
	if !merged.Window.End.Equal(testBase.Add(23*time.Second)) ||
		merged.Value != 15 || merged.Count != 5 {
		t.Fatalf("multi-round merged session wrong: %+v", merged)
	}
	if _, ok := vr[merged.ResultID]; !ok {
		t.Fatalf("reverse run missing merged identity %s: %+v", merged.ResultID, vr)
	}

	// 撤销总数与桥接轮次的先后无关：第一轮撤掉被并入的两个独立已发布会话
	//（2 个）；第二轮把上一轮的合并结果与第三个已发布会话再并成一个，
	// 两个旧标识都已发出过，再撤 2 个——共 4 个。
	countKinds := func(out []aggregate.Output) (ins, corr, ret int) {
		for _, o := range out {
			switch o.Kind {
			case aggregate.Insert:
				ins++
			case aggregate.Correction:
				corr++
			case aggregate.Retract:
				ret++
			}
		}
		return
	}
	_, _, retF := countKinds(fwd)
	_, _, retR := countKinds(rev)
	if retF != 4 || retR != 4 {
		t.Fatalf("retractions fwd=%d rev=%d, want 4 each", retF, retR)
	}

	// 最终各自行集合一致（不依赖到达顺序）。
	for id, ro := range vf {
		go2, ok := vr[id]
		if !ok || go2.Value != ro.Value || go2.Count != ro.Count ||
			!go2.Window.Start.Equal(ro.Window.Start) || !go2.Window.End.Equal(ro.Window.End) {
			t.Fatalf("reverse final row differs for %s:\n %+v\n %+v", id, ro, go2)
		}
	}
}
