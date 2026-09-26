package engine

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/stream-window-engine-go/internal/aggregate"
	"github.com/lechandonga/stream-window-engine-go/internal/event"
	"github.com/lechandonga/stream-window-engine-go/internal/sink"
	"github.com/lechandonga/stream-window-engine-go/internal/source"
	"github.com/lechandonga/stream-window-engine-go/internal/state"
	"github.com/lechandonga/stream-window-engine-go/internal/window"
)

func retractCfg(gap time.Duration) Config {
	return Config{
		Window:          window.Spec{Kind: window.Session, Gap: gap},
		Lag:             0,
		Late:            event.LateInclude,
		AllowedLateness: 24 * time.Hour, // 宽限覆盖全部迟到桥接
		Invalid:         event.InvalidReject,
		FutureBound:     24 * time.Hour,
		State:           state.Config{},
	}
}

// chainBridge 在 (from,to] 上每 step 秒放一个事件，间隔恰好 <= gap，
// 把两端的会话连成一个。
func chainBridge(prefix, key string, from, to, step int64) []event.Event {
	var evs []event.Event
	for ts := from; ts <= to; ts += step {
		evs = append(evs, eid(prefix+"-"+itoa(ts), key, ts, 1))
	}
	return evs
}

// runPublishedThenMerge 构造"两个会话先分别发布，再被迟到数据合并"
// 的输入，并返回完整输出。checkpointEvery 设为每条一个屏障，
// 使前两条会话在迟到桥接前就已经发布。
func runPublishedThenMerge(t *testing.T, cfg Config, events []event.Event, cpEvery int64) []aggregate.Output {
	t.Helper()
	if cpEvery > 0 {
		cfg.CheckpointDir = t.TempDir()
		cfg.CheckpointEvery = cpEvery
	}
	e := quietEngine(t, cfg)
	dst := sink.NewMemory()
	if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
		t.Fatal(err)
	}
	return dst.Outputs()
}

// 核心链路：两个已发布会话被迟到桥接合并 ->
// 旧结果显式 RETRACT（带 ResultID/Revision/ReplacedBy），
// 存活结果以 CORRECTION 承载合并值；下游按消息对账后只剩一条记录。
func TestLateMergeRetractsPublishedResult(t *testing.T) {
	gap := 10 * time.Second
	events := []event.Event{
		eid("a", "k", 0, 100),   // 会话 A [0,10)
		eid("b", "k", 100, 200), // 会话 B [100,110)
		eid("adv", "k", 200, 0), // 水位线推进：A、B 此时各自发布
	}
	events = append(events, chainBridge("br", "k", 10, 100, 10)...)

	out := runPublishedThenMerge(t, retractCfg(gap), events, 1)

	idA := ResultID("k", window.Window{Kind: window.Session, Start: testBase.Add(0), End: testBase.Add(10 * time.Second)})
	idB := ResultID("k", window.Window{Kind: window.Session, Start: testBase.Add(100 * time.Second), End: testBase.Add(110 * time.Second)})

	var retractB *aggregate.Output
	var finalA *aggregate.Output
	for i := range out {
		o := out[i]
		if o.Kind == aggregate.Retract && o.ResultID == idB {
			retractB = &out[i]
		}
		if o.ResultID == idA && o.Kind != aggregate.Retract {
			cp := out[i]
			finalA = &cp
		}
	}
	if retractB == nil {
		t.Fatalf("expected an explicit RETRACT of B, outputs: %+v", out)
	}
	if retractB.ReplacedBy != idA {
		t.Fatalf("retraction must point replaced_by=%s, got %q", idA, retractB.ReplacedBy)
	}
	if retractB.Revision != 1 || retractB.Value != 200 || retractB.Count != 1 {
		t.Fatalf("retraction must carry B's last published snapshot, got %+v", retractB)
	}
	if !retractB.Late {
		t.Fatalf("retraction caused by late merge must be marked late")
	}
	if finalA == nil || finalA.Kind != aggregate.Correction {
		t.Fatalf("survivor A must publish a CORRECTION replacement, got %+v", finalA)
	}
	// 合并值：100(A) + 200(B) + 10 个桥接事件 = 310，计数 12。
	if finalA.Value != 310 || finalA.Count != 12 {
		t.Fatalf("merged survivor value/count wrong: %+v", finalA)
	}
	if finalA.Window.End.Sub(testBase) != 110*time.Second {
		t.Fatalf("merged survivor end wrong: %v", finalA.Window.End)
	}

	// 下游视图对账模拟：INSERT/CORRECTION upsert，RETRACT 删除。
	view := map[string]aggregate.Output{}
	for _, o := range out {
		switch o.Kind {
		case aggregate.Retract:
			delete(view, o.ResultID)
		default:
			view[o.ResultID] = o
		}
	}
	if _, ghost := view[idB]; ghost {
		t.Fatalf("ghost record for retracted B must not remain: %+v", view[idB])
	}
	if len(view) != 2 { // 合并后的 A + adv 的空会话
		t.Fatalf("reconciled view must contain exactly the live sessions, got %d: %+v", len(view), view)
	}
	if got := view[idA]; got.Value != 310 {
		t.Fatalf("live survivor value wrong: %d", got.Value)
	}
}

// 边界验收：相邻两个会话"事件点间隔"恰好等于 gap 上限时必须算同一个
// （迟到桥接 -> 撤销旧会话）；严格大于 gap 时必须保持两个独立会话，
// 既不撤销也不产生幽灵记录。
func TestSessionGapBoundaryMergeAndSplit(t *testing.T) {
	gap := 10 * time.Second

	// 恰好 == gap：事件 0 与 10。先让 [0,10) 发布，再让 10 迟到。
	t.Run("touching_equal_gap_merges", func(t *testing.T) {
		events := []event.Event{
			eid("a", "g", 0, 100),
			eid("adv1", "g", 50, 0), // 发布 [0,10)
			eid("b", "g", 10, 200),  // 间隔恰好 == gap，迟到
			eid("adv2", "g", 60, 0),
		}
		out := runPublishedThenMerge(t, retractCfg(gap), events, 1)
		// b@10 与 a@0 同会话：同 ResultID，不应有针对 b 的独立结果或撤销；
		// 只应看到 [0,20) 合并会话的 CORRECTION。
		idA := ResultID("g", window.Window{Kind: window.Session, Start: testBase})
		var kinds []aggregate.OutputKind
		var retracts int
		var corr *aggregate.Output
		for i := range out {
			o := out[i]
			if o.ResultID == idA {
				kinds = append(kinds, o.Kind)
				if o.Kind == aggregate.Correction {
					cp := out[i]
					corr = &cp
				}
			}
			if o.Kind == aggregate.Retract {
				retracts++
			}
		}
		if retracts != 0 {
			t.Fatalf("touching events form one lineage; no retraction expected, got %d", retracts)
		}
		if corr == nil || corr.Value != 300 || corr.Count != 2 {
			t.Fatalf("expected one merged CORRECTION 300/2, kinds=%v corr=%+v", kinds, corr)
		}
		if corr.Window.End.Sub(testBase) != 20*time.Second {
			t.Fatalf("merged end = %v, want +20s", corr.Window.End.Sub(testBase))
		}
	})

	// 严格 > gap：事件 0 与 11（gap=10）必须保持两个独立会话。
	t.Run("strictly_greater_gap_splits", func(t *testing.T) {
		events := []event.Event{
			eid("a", "g", 0, 100),
			eid("b", "g", 11, 200), // 间隔 11s > gap
		}
		cfg := retractCfg(gap)
		cfg.Lag = 0
		e := quietEngine(t, cfg)
		dst := sink.NewMemory()
		if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
			t.Fatal(err)
		}
		out := dst.Outputs()
		var retracts int
		ends := map[int64]bool{}
		for _, o := range out {
			if o.Kind == aggregate.Retract {
				retracts++
			}
			ends[o.Window.End.Sub(testBase).Nanoseconds()] = true
		}
		if retracts != 0 {
			t.Fatalf("independent sessions must not be retracted, got %d", retracts)
		}
		if len(out) != 2 || !ends[10e9] || !ends[21e9] {
			t.Fatalf("want two independent sessions [0,10) and [11,21), got %+v", out)
		}
	})

	// 已发布会话场景下，桥接事件间隔恰好 == gap 时也要合并并撤销旧的第二个会话。
	t.Run("published_touching_bridge_retracts", func(t *testing.T) {
		events := []event.Event{
			eid("a", "g", 0, 100),
			eid("b", "g", 20, 200),
			eid("adv", "g", 100, 0), // A [0,10)、B [20,30) 各自发布
			eid("mid", "g", 10, 50), // 10-0==gap 且 20-10==gap：恰好连成一个
		}
		out := runPublishedThenMerge(t, retractCfg(gap), events, 1)
		idB := ResultID("g", window.Window{Kind: window.Session, Start: testBase.Add(20 * time.Second)})
		var rb *aggregate.Output
		for i := range out {
			if out[i].Kind == aggregate.Retract && out[i].ResultID == idB {
				rb = &out[i]
			}
		}
		if rb == nil {
			t.Fatalf("B must be retracted when touching bridge connects A-B, out=%+v", out)
		}
		if rb.ReplacedBy != ResultID("g", window.Window{Kind: window.Session, Start: testBase}) {
			t.Fatalf("retraction replaced_by wrong: %q", rb.ReplacedBy)
		}
	})
}

// canonicalSig 提取输出序列中与"下游对账"相关的全部字段，
// 用于逐条比较不同并发/重放下的序列一致性。
func canonicalSig(out []aggregate.Output) []string {
	var s []string
	for _, o := range out {
		s = append(s, o.Kind.String()+"|"+o.ResultID+"|"+itoa(o.Revision)+
			"|"+itoa(o.Window.Start.Sub(testBase).Nanoseconds())+
			"|"+itoa(o.Window.End.Sub(testBase).Nanoseconds())+
			"|"+itoa(o.Value)+"|"+itoa(o.Count)+"|"+o.ReplacedBy)
	}
	return s
}

// 不同并发度下，含撤销的完整输出序列（顺序与内容）必须逐条一致。
func TestRetractionSequenceConcurrencyInvariant(t *testing.T) {
	mkEvents := func() []event.Event {
		evs := []event.Event{
			// 每组两个先发布的分离会话。
			eid("g1-a", "g1", 0, 1),
			eid("g1-b", "g1", 100, 2),
			eid("g2-a", "g2", 0, 3),
			eid("g2-b", "g2", 100, 4),
			eid("g3-a", "g3", 5, 5),
			eid("g3-b", "g3", 105, 6),
			eid("adv", "g1", 200, 0), // 推进水位线（同批顺序观测）
		}
		// 让全部分组的水位线都越过会话 End。
		evs = append(evs,
			eid("adv-g2", "g2", 200, 0),
			eid("adv-g3", "g3", 200, 0),
		)
		// 迟到链式桥接，把每个分组的两个会话合并（产生撤销）。
		for ts := int64(10); ts <= 100; ts += 10 {
			evs = append(evs,
				eid("g1-br-"+itoa(ts), "g1", ts, 1),
				eid("g2-br-"+itoa(ts), "g2", ts, 1),
				eid("g3-br-"+itoa(ts), "g3", ts, 1),
			)
		}
		return evs
	}
	evs := mkEvents()
	run := func(conc int) []string {
		cfg := retractCfg(10 * time.Second)
		cfg.Concurrency = conc
		out := runPublishedThenMerge(t, cfg, evs, 1)
		return canonicalSig(out)
	}
	serial := run(1)
	for _, conc := range []int{2, 4, 8} {
		got := run(conc)
		if len(got) != len(serial) {
			t.Fatalf("conc=%d len=%d want %d", conc, len(got), len(serial))
		}
		for i := range serial {
			if got[i] != serial[i] {
				t.Fatalf("conc=%d divergence at %d:\n serial=%s\n got=%s", conc, i, serial[i], got[i])
			}
		}
	}
	// 序列中必须确实包含撤销，否则该测试无意义。
	var retracts int
	for _, s := range serial {
		if len(s) >= 7 && s[:7] == "RETRACT" {
			retracts++
		}
	}
	if retracts != 3 {
		t.Fatalf("expected exactly 3 retractions (one per group), got %d: %v", retracts, serial)
	}
}

// 中断 -> 检查点续跑：撤销既不遗漏也不重复，被撤销结果不复活，
// 完整序列与一次跑完逐条一致。
func TestRetractionAcrossRestart(t *testing.T) {
	gap := 10 * time.Second
	events := []event.Event{
		eid("a", "k", 0, 100),
		eid("b", "k", 100, 200),
		eid("adv", "k", 200, 0),
	}
	events = append(events, chainBridge("br", "k", 10, 100, 10)...)

	// 参考：一次跑完（每事件一个屏障，与中断/恢复对齐）。
	refOut := runPublishedThenMerge(t, retractCfg(gap), events, 1)
	refSig := canonicalSig(refOut)

	dir := t.TempDir()
	mkCfg := func() Config {
		c := retractCfg(gap)
		c.CheckpointDir = dir
		c.CheckpointEvery = 1
		return c
	}

	// 在"撤销尚未发出"的位置中断：前 3 条（A、B 已发布，桥接未开始）。
	e1 := quietEngine(t, mkCfg())
	src1 := source.NewMemoryWithFailure(events, 3)
	dst1 := sink.NewMemory()
	if err := e1.Run(context.Background(), src1, dst1); err == nil {
		t.Fatal("expected source interruption")
	}
	if n := len(dst1.Outputs()); n != 2 {
		t.Fatalf("before interruption want 2 publishes, got %d", n)
	}

	// 续跑：预置已确认输出，新引擎从检查点恢复。
	e2 := quietEngine(t, mkCfg())
	dst2 := sink.NewMemory()
	for _, o := range dst1.Outputs() {
		if err := dst2.Deliver(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := e2.Run(context.Background(), source.NewMemory(events), dst2); err != nil {
		t.Fatalf("resume: %v", err)
	}
	gotSig := canonicalSig(dst2.Outputs())
	if len(gotSig) != len(refSig) {
		t.Fatalf("resume len=%d want %d", len(gotSig), len(refSig))
	}
	for i := range refSig {
		if gotSig[i] != refSig[i] {
			t.Fatalf("resume divergence at %d:\n ref=%s\n got=%s", i, refSig[i], gotSig[i])
		}
	}

	// 再跑第三次（模拟重复恢复/重放）：输出不得再增长，撤销不重复。
	before := len(dst2.Outputs())
	e3 := quietEngine(t, mkCfg())
	dst3 := sink.NewMemory()
	for _, o := range dst2.Outputs() {
		if err := dst3.Deliver(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := e3.Run(context.Background(), source.NewMemory(events), dst3); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(dst3.Outputs()) != before {
		t.Fatalf("replay must not re-emit (got %d, want %d)", len(dst3.Outputs()), before)
	}
	var retractSeqs []int64
	for _, o := range dst3.Outputs() {
		if o.Kind == aggregate.Retract {
			retractSeqs = append(retractSeqs, o.Seq)
		}
	}
	if len(retractSeqs) != 1 {
		t.Fatalf("exactly one retraction across restart+replay, got %d", len(retractSeqs))
	}

	// 对账后 B 不复活。
	view := map[string]aggregate.Output{}
	for _, o := range dst3.Outputs() {
		if o.Kind == aggregate.Retract {
			delete(view, o.ResultID)
		} else {
			view[o.ResultID] = o
		}
	}
	idB := ResultID("k", window.Window{Kind: window.Session, Start: testBase.Add(100 * time.Second)})
	if _, alive := view[idB]; alive {
		t.Fatal("retracted result B must not resurrect after restart/replay")
	}
}

// 中断点恰好跨过"撤销已发出"的屏障：续跑后撤销不重复、
// 后续修正不遗漏，终态与一次跑完一致。
func TestRetractionRestartAfterRetractEmitted(t *testing.T) {
	gap := 10 * time.Second
	events := []event.Event{
		eid("a", "k", 0, 100),
		eid("b", "k", 100, 200),
		eid("adv", "k", 200, 0),
	}
	events = append(events, chainBridge("br", "k", 10, 100, 10)...) // 共 13 条
	refSig := canonicalSig(runPublishedThenMerge(t, retractCfg(gap), events, 1))

	dir := t.TempDir()
	mkCfg := func() Config {
		c := retractCfg(gap)
		c.CheckpointDir = dir
		c.CheckpointEvery = 1
		return c
	}
	// 前 12 条：桥接已推进到 br90，此时 A 已与 B 合并、撤销已发出，
	// 仅剩 br100 与收尾未处理。
	const failAfter = 12
	e1 := quietEngine(t, mkCfg())
	dst1 := sink.NewMemory()
	if err := e1.Run(context.Background(), source.NewMemoryWithFailure(events, failAfter), dst1); err == nil {
		t.Fatal("expected interruption")
	}
	sawRetract := false
	for _, o := range dst1.Outputs() {
		if o.Kind == aggregate.Retract {
			sawRetract = true
		}
	}
	if !sawRetract {
		t.Fatal("this interruption point assumes the retraction was already emitted")
	}

	e2 := quietEngine(t, mkCfg())
	dst2 := sink.NewMemory()
	for _, o := range dst1.Outputs() {
		if err := dst2.Deliver(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := e2.Run(context.Background(), source.NewMemory(events), dst2); err != nil {
		t.Fatalf("resume: %v", err)
	}
	gotSig := canonicalSig(dst2.Outputs())
	if len(gotSig) != len(refSig) {
		t.Fatalf("len %d want %d", len(gotSig), len(refSig))
	}
	for i := range refSig {
		if gotSig[i] != refSig[i] {
			t.Fatalf("divergence at %d:\n ref=%s\n got=%s", i, refSig[i], gotSig[i])
		}
	}
	nRetract := 0
	for _, o := range dst2.Outputs() {
		if o.Kind == aggregate.Retract {
			nRetract++
		}
	}
	if nRetract != 1 {
		t.Fatalf("retraction duplicated across resume: %d", nRetract)
	}
}

// 内存上限（OverflowSpill / OverflowBlock）下，含撤销的输出序列
// 必须与无限内存完全一致；重启重放不重发撤销。
//
// 两种上限策略的容量约束不同：
//   - OverflowSpill 可把冷状态透明落盘，故可用极小上限（2）；
//   - OverflowBlock 在宽限期内对"同时存活的窗口数"有下界
//     （迟到合并需要新旧会话短暂并存），这里用足够容纳活跃窗口
//     的较小上限（8）验证有界配置下结果序列不变；
//     极端小上限下的背压完成性由 TestBackpressureCompletes 覆盖。
func TestRetractionUnderMemoryLimit(t *testing.T) {
	gap := 10 * time.Second
	events := []event.Event{
		eid("a", "k", 0, 100),
		eid("b", "k", 100, 200),
		eid("adv", "k", 200, 0),
	}
	events = append(events, chainBridge("br", "k", 10, 100, 10)...)
	ref := canonicalSig(runPublishedThenMerge(t, retractCfg(gap), events, 1))

	cases := []struct {
		name string
		pol  state.OverflowPolicy
		capN int
	}{
		{"spill", state.OverflowSpill, 2},
		{"block", state.OverflowBlock, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := retractCfg(gap)
			cfg.CheckpointDir = t.TempDir()
			cfg.CheckpointEvery = 1
			cfg.State = state.Config{MaxActiveKeys: tc.capN, Overflow: tc.pol, SpillDir: t.TempDir()}
			e := quietEngine(t, cfg)
			dst := sink.NewMemory()
			if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
				t.Fatal(err)
			}
			got := canonicalSig(dst.Outputs())
			if len(got) != len(ref) {
				t.Fatalf("%s: len %d want %d", tc.name, len(got), len(ref))
			}
			for i := range ref {
				if got[i] != ref[i] {
					t.Fatalf("%s: divergence at %d:\n ref=%s\n got=%s", tc.name, i, ref[i], got[i])
				}
			}
			// 重放：落盘/背压状态从检查点重建后撤销不重发。
			e2 := quietEngine(t, cfg)
			dst2 := sink.NewMemory()
			for _, o := range dst.Outputs() {
				if err := dst2.Deliver(o); err != nil {
					t.Fatal(err)
				}
			}
			if err := e2.Run(context.Background(), source.NewMemory(events), dst2); err != nil {
				t.Fatalf("%s replay: %v", tc.name, err)
			}
			if len(dst2.Outputs()) != len(dst.Outputs()) {
				t.Fatalf("%s: re-emission after replay", tc.name)
			}
		})
	}
}

// 撤销元数据有界：Purged 已关闭会话不携带任何待发撤销，
// 长时间运行后检查点中也不存在残留 Pending。
func TestRetractionMetadataBounded(t *testing.T) {
	gap := 5 * time.Second
	// 造一批"先分离发布、随后被迟到桥接合并、再彻底过期"的会话。
	var evs []event.Event
	for g := 0; g < 4; g++ {
		key := "g" + itoa(int64(g))
		evs = append(evs,
			eid(key+"-a", key, int64(g*1000), 1),
			eid(key+"-b", key, int64(g*1000+100), 2),
			eid(key+"-adv", key, int64(g*1000+500), 0),
		)
	}
	// 迟到桥接，合并每对已发布会话（产生撤销）。
	for g := 0; g < 4; g++ {
		key := "g" + itoa(int64(g))
		base := int64(g * 1000)
		for ts := base + 5; ts <= base+100; ts += 5 {
			evs = append(evs, eid(key+"-br-"+itoa(ts), key, ts, 1))
		}
	}
	// 大幅推进水位线越过全部窗口+宽限期，触发关闭与清理。
	evs = append(evs, eid("far-future-1", "g0", 100000, 0), eid("far-future-2", "g1", 100001, 0))

	cfg := retractCfg(gap)
	// 宽限期要同时满足：迟到桥接被接受（产生撤销），且远期水位线
	// 能越过 End+宽限 触发关闭与清理。
	cfg.AllowedLateness = 2000 * time.Second
	cfg.CheckpointDir = t.TempDir()
	cfg.CheckpointEvery = 1
	e := quietEngine(t, cfg)
	dst := sink.NewMemory()
	if err := e.Run(context.Background(), source.NewMemory(evs), dst); err != nil {
		t.Fatal(err)
	}

	// 至少产生了撤销。
	nRetract := 0
	for _, o := range dst.Outputs() {
		if o.Kind == aggregate.Retract {
			nRetract++
		}
	}
	if nRetract == 0 {
		t.Fatal("test setup should produce retractions")
	}
	// 终态存储中不得残留任何 Pending 撤销。
	keys, err := e.store.ListKeys(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		en, ok, err := e.store.Get(context.Background(), k)
		if err != nil || !ok {
			t.Fatal(err)
		}
		if len(en.Pending) != 0 {
			t.Fatalf("pending retraction leaked past fire/purge: %s %d", k.ID(), len(en.Pending))
		}
	}
	// 活动状态条目有界：旧会话应被清理，只剩极近期窗口。
	if n := e.store.ActiveCount(); n > 8 {
		t.Fatalf("state not bounded after purge: active=%d", n)
	}
}

// 幂等消费：
//  1. 同一事件被重复投递（含迟到路径）不重复计数、不产生额外撤销；
//  2. 同一输出（含撤销）按 Seq 重复投递到 sink 只生效一次；
//  3. 下游按 (ResultID, Revision) 对账时，重复撤销不改变终态。
func TestRetractionIdempotentConsumption(t *testing.T) {
	gap := 10 * time.Second
	events := []event.Event{
		eid("a", "k", 0, 100),
		eid("b", "k", 100, 200),
		eid("adv", "k", 200, 0),
	}
	events = append(events, chainBridge("br", "k", 10, 100, 10)...)
	// 重复投递：桥接事件与已发布会话事件各重放一遍（迟到路径）。
	dup := []event.Event{
		eid("a", "k", 0, 100),
		eid("br-50", "k", 50, 1),
		eid("b", "k", 100, 200),
	}
	withDup := append(append([]event.Event{}, events...), dup...)

	clean := canonicalSig(runPublishedThenMerge(t, retractCfg(gap), events, 1))
	dirty := canonicalSig(runPublishedThenMerge(t, retractCfg(gap), withDup, 1))
	if len(clean) != len(dirty) {
		t.Fatalf("duplicate redelivery changed output count: %d vs %d", len(clean), len(dirty))
	}
	for i := range clean {
		if clean[i] != dirty[i] {
			t.Fatalf("duplicate redelivery divergence at %d:\n clean=%s\n dirty=%s", i, clean[i], dirty[i])
		}
	}

	// sink 侧：同一 Seq 的撤销重复投递只记录一次。
	ms := sink.NewMemory()
	out := runPublishedThenMerge(t, retractCfg(gap), events, 1)
	var retract *aggregate.Output
	for i := range out {
		if out[i].Kind == aggregate.Retract {
			retract = &out[i]
		}
	}
	if retract == nil {
		t.Fatal("setup must produce a retraction")
	}
	if err := ms.Deliver(*retract); err != nil {
		t.Fatal(err)
	}
	if err := ms.Deliver(*retract); err != nil { // 重复投递
		t.Fatal(err)
	}
	if n := len(ms.Outputs()); n != 1 {
		t.Fatalf("sink must dedupe by Seq, got %d outputs", n)
	}

	// 下游对账侧：重复应用同一撤销是幂等的（删除已删除的键无效果）。
	view := map[string]int64{}
	for _, o := range out {
		if o.Kind == aggregate.Retract {
			delete(view, o.ResultID)
			delete(view, o.ResultID) // 重复撤销
		} else {
			view[o.ResultID] = o.Value
		}
	}
	idB := ResultID("k", window.Window{Kind: window.Session, Start: testBase.Add(100 * time.Second)})
	if _, ok := view[idB]; ok {
		t.Fatal("retracted id must stay deleted under duplicate retraction")
	}
}

// 会话合并后的迟到重复投递：事件原属会话已被并入存活会话，
// 其重复投递必须被去重（合并按事件 ID 去重），不得二次计数。
func TestMergedSessionLateDuplicateNotDoubleCounted(t *testing.T) {
	gap := 10 * time.Second
	events := []event.Event{
		eid("a", "k", 0, 100),
		eid("b", "k", 100, 200),
		eid("adv", "k", 200, 0),
	}
	events = append(events, chainBridge("br", "k", 10, 100, 10)...)
	// b 的会话已被并入 A 的谱系；此时再迟到重投 b。
	events = append(events, eid("b", "k", 100, 200))

	out := runPublishedThenMerge(t, retractCfg(gap), events, 1)
	idA := ResultID("k", window.Window{Kind: window.Session, Start: testBase})
	var finalA *aggregate.Output
	for i := range out {
		if out[i].ResultID == idA && out[i].Kind != aggregate.Retract {
			cp := out[i]
			finalA = &cp
		}
	}
	if finalA == nil {
		t.Fatal("missing survivor output")
	}
	// 与无重复场景一致：310/12，b 的重投不得再 +200。
	if finalA.Value != 310 || finalA.Count != 12 {
		t.Fatalf("late duplicate of merged event double counted: %+v", finalA)
	}
}

// 滚动/滑动窗口：ResultID 稳定（同窗口 INSERT 与 CORRECTION 同标识），
// 且这两类窗口永不产生撤销；既有输出字段与行为保持兼容。
func TestResultIDStableForTumblingAndSliding(t *testing.T) {
	events := []event.Event{
		eid("a", "k", 1, 10),
		eid("adv", "k", 25, 0),
		eid("late", "k", 5, 7), // 迟到修正 [0,10)
	}
	for _, kind := range []window.Kind{window.Tumbling, window.Sliding} {
		name := "tumbling"
		spec := window.Spec{Kind: window.Tumbling, Size: 10 * time.Second}
		if kind == window.Sliding {
			name = "sliding"
			spec = window.Spec{Kind: window.Sliding, Size: 10 * time.Second, Slide: 5 * time.Second}
		}
		t.Run(name, func(t *testing.T) {
			cfg := Config{
				Window:          spec,
				Lag:             0,
				Late:            event.LateInclude,
				AllowedLateness: 30 * time.Second,
				Invalid:         event.InvalidReject,
				FutureBound:     24 * time.Hour,
				State:           state.Config{},
				// 逐事件屏障：让首版先发布，迟到事件才产生独立 CORRECTION。
				CheckpointDir:   t.TempDir(),
				CheckpointEvery: 1,
			}
			e := quietEngine(t, cfg)
			dst := sink.NewMemory()
			if err := e.Run(context.Background(), source.NewMemory(events), dst); err != nil {
				t.Fatal(err)
			}
			byID := map[string][]aggregate.Output{}
			for _, o := range dst.Outputs() {
				if o.Kind == aggregate.Retract {
					t.Fatalf("%s windows must never retract: %+v", name, o)
				}
				if o.ResultID == "" {
					t.Fatalf("ResultID must be populated: %+v", o)
				}
				byID[o.ResultID] = append(byID[o.ResultID], o)
			}
			// 找到被迟到修正的窗口：同 ResultID 下 INSERT 后接 CORRECTION，
			// 修订号递增。
			found := false
			for _, outs := range byID {
				if len(outs) == 2 && outs[0].Kind == aggregate.Insert && outs[1].Kind == aggregate.Correction {
					found = true
					if outs[1].Revision != outs[0].Revision+1 {
						t.Fatalf("revision must increment within same ResultID: %+v", outs)
					}
					if outs[0].Window != outs[1].Window {
						t.Fatalf("fixed window identity changed across revisions: %+v", outs)
					}
				}
			}
			if !found {
				t.Fatalf("%s: expected an INSERT+CORRECTION lineage sharing ResultID", name)
			}
		})
	}
}

// 同一逻辑结果的完整生命周期：INSERT -> CORRECTION（迟到延长）->
// RETRACT（被合并撤销）由同一 ResultID 串起；被撤销的是"已发布的
// 最新修订"。
func TestResultLifecycleSingleLineage(t *testing.T) {
	gap := 10 * time.Second
	events := []event.Event{
		eid("a", "k", 0, 1),
		eid("b", "k", 100, 2),
		eid("adv", "k", 200, 0),
		eid("late-extend", "k", 5, 3), // 迟到延长 A -> CORRECTION
		eid("br1", "k", 10, 1),
		eid("br2", "k", 20, 1),
		eid("br3", "k", 30, 1),
		eid("br4", "k", 40, 1),
		eid("br5", "k", 50, 1),
		eid("br6", "k", 60, 1),
		eid("br7", "k", 70, 1),
		eid("br8", "k", 80, 1),
		eid("br9", "k", 90, 1),
		eid("br10", "k", 100, 1), // 连到 B -> 撤销 B
	}
	out := runPublishedThenMerge(t, retractCfg(gap), events, 1)
	idA := ResultID("k", window.Window{Kind: window.Session, Start: testBase})
	idB := ResultID("k", window.Window{Kind: window.Session, Start: testBase.Add(100 * time.Second)})

	// A 谱系：INSERT rev1 -> CORRECTION rev2..（迟到延长与合并）同一 ResultID。
	var aRevs []int64
	for _, o := range out {
		if o.ResultID == idA && o.Kind != aggregate.Retract {
			aRevs = append(aRevs, o.Revision)
		}
	}
	if len(aRevs) < 3 || aRevs[0] != 1 {
		t.Fatalf("A lineage revisions wrong: %v", aRevs)
	}
	for i := 1; i < len(aRevs); i++ {
		if aRevs[i] != aRevs[i-1]+1 {
			t.Fatalf("A revisions must be contiguous: %v", aRevs)
		}
	}
	// B 谱系：INSERT rev1，随后唯一一条 RETRACT（rev1，replacedBy=A）。
	var bInserts, bRetracts int
	for _, o := range out {
		if o.ResultID == idB {
			switch o.Kind {
			case aggregate.Insert:
				bInserts++
			case aggregate.Retract:
				bRetracts++
				if o.Revision != 1 || o.ReplacedBy != idA {
					t.Fatalf("B retraction must reference last published rev and survivor: %+v", o)
				}
			}
		}
	}
	if bInserts != 1 || bRetracts != 1 {
		t.Fatalf("B lifecycle must be INSERT then exactly one RETRACT: ins=%d ret=%d", bInserts, bRetracts)
	}
}

// 级联合并：三个先分别发布的会话被迟到数据连成一个。
// 每一个被吸收的旧会话都必须被撤销，且 replacedBy 全部指向最终
// 存活谱系；中途挂在中间会话上的撤销也要转移到最终存活者。
func TestCascadeMergeRetractsAllAbsorbed(t *testing.T) {
	gap := 10 * time.Second
	events := []event.Event{
		eid("a", "k", 0, 1),
		eid("b", "k", 100, 2),
		eid("c", "k", 200, 4),
		eid("adv", "k", 400, 0), // 三个会话分别发布
	}
	events = append(events, chainBridge("br", "k", 10, 200, 10)...)

	out := runPublishedThenMerge(t, retractCfg(gap), events, 1)
	idA := ResultID("k", window.Window{Kind: window.Session, Start: testBase})
	idB := ResultID("k", window.Window{Kind: window.Session, Start: testBase.Add(100 * time.Second)})
	idC := ResultID("k", window.Window{Kind: window.Session, Start: testBase.Add(200 * time.Second)})

	retracted := map[string]string{} // resultID -> replacedBy
	for _, o := range out {
		if o.Kind == aggregate.Retract {
			if _, dup := retracted[o.ResultID]; dup {
				t.Fatalf("duplicate retraction for %s", o.ResultID)
			}
			retracted[o.ResultID] = o.ReplacedBy
		}
	}
	if len(retracted) != 2 {
		t.Fatalf("exactly B and C must be retracted, got %d: %+v", len(retracted), retracted)
	}
	if retracted[idB] != idA || retracted[idC] != idA {
		t.Fatalf("both retractions must point to final survivor A: %+v", retracted)
	}
	// 存活谱系最终承载全部去重事件：1+2+4 + 20 个桥接(10..200) = 27，计数 23。
	var final *aggregate.Output
	for i := range out {
		if out[i].ResultID == idA && out[i].Kind != aggregate.Retract {
			cp := out[i]
			final = &cp
		}
	}
	if final == nil || final.Value != 27 || final.Count != 23 {
		t.Fatalf("cascade survivor wrong: %+v", final)
	}
}
