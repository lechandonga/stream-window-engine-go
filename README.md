# Stream Window Engine (Go)

一个小型**本地流式事件聚合引擎**。用内存/本地文件模拟事件源与结果输出，
保证同一批事件在**不同到达顺序、不同并发度与重启场景**下得到一致、
可解释、可重放的结果。

- 纯 Go 标准库实现，无第三方依赖；
- 结构化日志（`log/slog`）记录每个窗口的归属、水位线推进与判定依据；
- 输出区分**首次（INSERT）/ 迟到修正（CORRECTION）/ 撤销（RETRACT）**，
  下游可增量维护「分组+窗口」视图而不留幽灵记录；
- 全部保证均有测试覆盖（乱序、迟到、重复投递、撤销、重启、内存限额、损坏检测）。

---

## 1. 窗口语义

所有窗口都是**半开区间 `[Start, End)`**：恰好落在边界上的事件归属**后一个**窗口。
窗口边界相对 epoch（或显式 `Offset`）对齐，因此窗口归属只由事件时间决定，
与到达顺序无关。

| 类型 | 配置 | 归属 |
|---|---|---|
| 滚动 Tumbling | `Size` | 事件恰好属于 1 个窗口 |
| 滑动 Sliding | `Size` + `Slide` | 事件可同时属于多个窗口（`Size/Slide` 个） |
| 会话 Session | `Gap` | 初始单事件会话 `[ts, ts+Gap)`，相邻间隔 `<= Gap` 的会话确定地合并 |

会话合并规则与输入顺序无关：按 `Start`（再按 `End`、`Seq`）排序后聚类，
合并区间取并、`Seq` 取成员最小值，因此任何重放都得到相同会话。

### 异常时间事件（绝不静默丢弃）

- 缺失事件时间（零值）→ `missing_event_time`
- 早于可信下界（默认 2000-01-01）→ `time_out_of_allowed_range`
- 远超处理时刻（超过 `FutureBound`）→ `event_time_beyond_future_bound`

处置由 `Invalid` 配置决定，但**任何**策略都会写入旁路记录（sink side-output）：

| 策略 | 行为 |
|---|---|
| `InvalidReject` | 拒绝，不参与聚合，仅记录 |
| `InvalidAssignWatermark` | 用当前水位线时间（未产生水位线时用处理时间） |
| `InvalidAssignProcessingTime` | 用处理时刻 |

---

## 2. 水位线语义

水位线 `W` 表示“引擎相信事件时间 `<= W` 的事件均已到达”。

```
W = maxObservedEventTime - Lag
```

- `Lag`（bounded out-of-orderness）是显式配置的乱序容忍；
- 水位线**单调不减，任何情况下都不回退**：更旧的事件时间、检查点恢复后
  重放历史事件，都不会降低它（见 `watermark.Watermark.Advance`）；
- 水位线在**屏障点**参与窗口触发：`watermark >= Window.End` 时触发窗口；
- 日志中每次推进都带 `watermark=`、`event_id=`、`event_ts=`。

### 迟到事件策略（`Late`）

水位线之后到达的事件不会被静默吞掉：

| 策略 | 行为 | 主输出 | 记录 |
|---|---|---|---|
| `LateDrop` | 丢弃 | 不变化 | `late_event_dropped` |
| `LateSideOutput` | 进侧输出 | 不变化 | `late_event_side_output` |
| `LateInclude` | 在 `AllowedLateness` 宽限期内纳入 | 产生 `CORRECTION` | 纳入日志；超宽限 `late_event_dropped` |

宽限判定：仅当 `watermark <= Window.End + AllowedLateness` 时迟到事件才纳入。

---

## 3. 输出语义：新增、修正与撤销

每个 `(Key, Window)` 的结果带：

- `Seq`：引擎内全局单调递增的**幂等键**，sink 按 `Seq` 去重，
  重复投递（含重启重放）不会产生重复结果；
- `Revision`：同一窗口的修订号，首版 `1`，每次迟到修正 `+1`；
- `Kind`：`INSERT`（首次）、`CORRECTION`（迟到导致的修正）或
  `RETRACT`（撤销，见下），下游可明确区分；
- `ResultID`：该结果的**稳定标识**（`Key|窗口种类:Start:End:Seq`），
  只由分组键与窗口决定，与运行次数、并发度、重启重放无关；
- `Late`、`Watermark`、`EmitAt`：解释这次输出由什么触发。

重复事件由窗口累加器按**事件 ID** 去重：同一 ID 二次进入不重复计数，
也不会造成重复修正（会话窗口同样成立：同组任一会话已见过该 ID 即跳过）。

### 撤销（RETRACT）：已发布结果被收回

会话窗口可能随（迟到）数据继续合并：两个已经分别发布过的会话，
会被一条迟到事件桥接成同一个更大的会话。此时旧标识的结果**不再成立**，
引擎在同一屏障内、替代结果**之前**发出 `RETRACT`：

```
seq=3 RETRACT key=room-A window=[00,05) rev=1 replaced_by=room-A|session:...:17...:1
seq=4 RETRACT key=room-A window=[12,17) rev=1 replaced_by=room-A|session:...:17...:1
seq=5 INSERT  key=room-A window=[00,17) rev=1            ← 替代结果（新标识）
```

- `RETRACT` 记录精确携带被撤销结果的 `ResultID`、被撤销时的
  `Revision`/`Value`/`Count`，以及替代结果的 `ResultID`（`ReplacedBy`），
  下游不需要自己猜旧结果是否还有效；
- 替代结果是**新标识**的 `INSERT rev=1`（每个标识有独立生命周期：
  `INSERT → CORRECTION… → RETRACT`）；
- 若迟到事件并入后窗口标识不变（新区间被既有会话完全覆盖），
  则走 `CORRECTION`，不产生撤销；
- 滚动/滑动窗口边界固定、不会消失，因此不会产生 `RETRACT`，
  既有输出序列完全不变。

**下游对账方式**（按 `Seq` 顺序应用即可）：

| 输出 | 视图操作 |
|---|---|
| `INSERT` / `CORRECTION` | `view[ResultID] = 本结果`（upsert） |
| `RETRACT` | `delete view[ResultID]`（不存在则 no-op） |
| 任何 `Seq` 已应用过的记录 | 跳过（幂等） |

由此：重复投递、重启重放、并发处理都不会让下游重复撤销，
也不会撤销掉仍然有效的结果（被撤标识精确匹配，且会话合并单调、
被撤标识不会复活）。撤销判定发生在会话合并时，发射统一在屏障点按
`(Group, Window.Start, End, Seq)` 排序进行，因此撤销序列同样与
并发度、重放次数、中断续跑无关。待撤销信息是屏障内瞬时状态
（发射即清空），不进检查点、不随运行时间增长；崩溃后随状态回退与
输入重放被等价重建，配合 `Seq` 幂等保证不重复、不遗漏。

---

## 4. 屏障、并发与确定性

处理以**屏障批次**进行（大小 = `min(32, CheckpointEvery)`）：

1. **预处理阶段（按源顺序，串行）**：时间异常分类、观测事件时间、冻结批水位线；
   会话窗口的窗口序号（`Window.Seq`）也在此阶段分配——同一事件在任何
   并发度、任何重放下得到相同序号，会话窗口标识（及 `ResultID`）因此稳定；
2. **写入阶段（屏障内并发）**：事件按分组键 FNV 哈希分片到 `Concurrency`
   个 worker，同键事件永远在同一 worker 保序；会话合并按分组键互斥；
3. **触发阶段（屏障点）**：先按 `(Group, Window.Start, End, Seq)` 确定性顺序
   发射本屏障的撤销（RETRACT），再以同样确定性键序触发窗口、分配 `Seq`。

因此：**并发执行与串行执行的输出序列逐字节一致**（有
`TestMultiGroupConcurrentEquivalence` 用乱序多键数据对比 concurrency=1/8；
`TestSessionRetractSameAcrossConcurrency` 对含撤销的会话输出对比 1/2/4/8）。

> 屏障位置只依赖输入中的绝对记录位置，与“在哪里中断/恢复”无关，
> 因此中断后续跑的输出序列与不间断运行对齐（在相同屏障调度下）。

---

## 5. 检查点与恢复

- 检查点文件 `chk.json`：魔数 + 紧凑 JSON 载荷 + **SHA-256 校验和**；
- 写入是原子的：写临时文件 → `fsync` → `rename`。中途失败不产生半成品，
  旧快照始终有效；
- 快照内容：水位线、`NextSeq`、**会话序号计数器（`SessionSeq`，保证
  重放后会话窗口标识不漂移；旧快照缺该字段时从存活条目反推，向前兼容）**、
  源偏移、全部窗口累加状态（含去重 ID 集合）、修订号、关闭/脏标记、落盘文件清单；
- 恢复规则：
  - 无检查点 → 冷启动；
  - 魔数/JSON/校验和/版本任一不符 → 返回 `checkpoint.ErrCorrupt`，
    **绝不带猜测状态继续**；
  - `NextSeq` 取快照值与 sink 已确认 `LastSeq` 的较大者 → 已确认输出不重发；
  - 水位线恢复后只增不减；未完成窗口（含落盘的）全部重建，不丢失；
- 来源中断时，不足一个屏障的残余批次整批不提交（源偏移停在上一屏障末尾），
  恢复后从源重放，避免半更新状态和提前触发。

可区分的失败类型：`source.ErrInterrupted`（来源中断）、
`checkpoint.ErrWriteFailed`（写入失败）、`checkpoint.ErrCorrupt`（损坏）。

---

## 6. 有界状态

`state.Config`：

| 字段 | 含义 |
|---|---|
| `MaxActiveKeys` | 内存中活动窗口键上限；`<=0` 不限 |
| `Overflow` | `OverflowBlock`（背压）或 `OverflowSpill`（LRU 冷状态落盘） |
| `SpillDir` | 落盘目录（`OverflowSpill` 必填） |

- `OverflowBlock`：达到上限时新键写入阻塞（响应 ctx 取消），直到彻底过期
  的窗口被 `Purge` 清理释放空间；
- `OverflowSpill`：最久未访问的状态原子写盘（临时文件 + rename），读回透明；
  落盘文件纳入检查点，重启后仍可恢复；损坏的落盘文件会被当作错误暴露；
- 水位线越过 `Window.End + AllowedLateness` 且已关闭/已发出的窗口才被清理，
  状态占用有明确上界；
- 撤销（RETRACT）不引入新的持久状态：待撤销列表是屏障内瞬时结构，
  发射后即清空；背压与落盘下撤销同样完整（见
  `TestSessionRetractUnderMemoryLimit`、`TestSessionRetractWithSpillAcrossRestart`）。

---

## 7. 配置项（`engine.Config`）

| 字段 | 说明 |
|---|---|
| `Window` | `window.Spec{Kind, Size, Slide, Gap, Offset}` |
| `Aggregator` | 内置 `"sum_count"`（求和+计数） |
| `Lag` | 水位线乱序容忍 |
| `Late` | `LateDrop` / `LateSideOutput` / `LateInclude` |
| `AllowedLateness` | `LateInclude` 的宽限期 |
| `Invalid` | `InvalidReject` / `InvalidAssignWatermark` / `InvalidAssignProcessingTime` |
| `FutureBound` | 事件时间相对处理时刻允许超前的最大幅度 |
| `State` | 内存上限 / 背压或落盘 / 落盘目录 |
| `CheckpointDir` | 检查点目录；为空则不做检查点 |
| `CheckpointEvery` | 每处理 N 条做一次检查点（也是屏障对齐周期） |
| `Concurrency` | 分组处理并发度（输出仍与串行等价） |

选项：`engine.WithLogger(*slog.Logger)`、`engine.WithClock(func() time.Time)`
（注入时钟，便于确定性测试）。

### 兼容范围（本轮新增撤销能力）

- **配置**：未新增、未修改任何配置项；既有配置行为不变；
- **数据格式**：`Output` 仅新增字段（`result_id`、`replaced_by`），
  既有字段名与含义不变，旧下游可安全忽略新字段；
- **输出约定**：`INSERT`/`CORRECTION` 语义不变；`RETRACT` 是新增 `Kind`，
  只在「已发布的会话结果被合并掉」时出现——滚动/滑动窗口输出序列完全不变；
- **检查点**：快照新增 `SessionSeq` 字段；读取旧快照时自动回退兼容，
  新版本写出的快照不建议用旧版本读取；
- **行为修复**（属于缺陷修复而非语义变更）：会话窗口下同一事件 ID 的
  重复投递/重启重放此前可能被二次计数，现与滚动/滑动窗口一致按 ID 去重。

---

## 8. 验证方法

```bash
# 全部单元测试
go test ./...

# 竞态检测（需要 cgo/gcc）
CGO_ENABLED=1 go test -race ./...

go vet ./...
```

测试覆盖重点（对应需求）：

| 主题 | 测试 |
|---|---|
| 窗口边界/归属确定性 | `window/window_test.go` |
| 水位线单调不回退 | `watermark/watermark_test.go` |
| 乱序聚合、INSERT/CORRECTION | `engine.TestOutOfOrderTumbling`、`TestLatePolicies` |
| 滑动/会话窗口 | `engine.TestSlidingWindow`、`TestSessionWindowOrderIndependent` |
| **迟到合并撤销已发布结果** | `engine.TestSessionMergeRetractsPublishedResults` |
| **撤销序列跨并发度一致（1/2/4/8）** | `engine.TestSessionRetractSameAcrossConcurrency` |
| **撤销重启/重放不重复不遗漏** | `engine.TestSessionRetractSurvivesRestartAndReplay` |
| **会话间隔边界（=Gap 合并 / >Gap 分离）** | `engine.TestSessionGapBoundary` |
| **同标识扩展走 CORRECTION 不撤销** | `engine.TestSessionExtensionSameIdentityCorrects` |
| **会话窗口重复投递去重** | `engine.TestSessionDuplicateRedelivery` |
| **背压/落盘下撤销完整** | `engine.TestSessionRetractUnderMemoryLimit`、`TestSessionRetractWithSpillAcrossRestart` |
| 重复投递幂等 | `aggregate.TestAccumulatorDedup`、`engine.TestDuplicateRedelivery`、`sink/*` |
| 重放序列一致 | `engine.TestReplayDeterminism`、`source.TestReplayFromOffsetIdentical` |
| 并发=串行 | `engine.TestMultiGroupConcurrentEquivalence`（-race） |
| 重启恢复、不重复发出 | `engine.TestRestartAfterInterruption`、`TestFileSinkNoDuplicateAcrossRestart` |
| 水位线恢复不回退 | `engine.TestWatermarkNeverRegressOnRestore` |
| 内存上限/背压/落盘 | `state/*_test.go`、`engine.TestSpillUnderMemoryLimit`、`TestBackpressureCompletes` |
| 来源中断 | `source.TestInterruptIsDetectable`、`engine.TestRestartAfterInterruption` |
| 写入失败/损坏检测 | `checkpoint/*_test.go`、`engine.TestCheckpointWriteFailure`、`TestCorruptCheckpointRejected` |
| 异常时间处理 | `engine.TestInvalidTimeRecorded`、`TestInvalidAssignPolicy` |

### Demo

```bash
# 完整运行（乱序/迟到/重复/异常时间）
go run ./cmd/demo -dir /tmp/swe-demo

# 会话窗口撤销演示：两个已发布会话被迟到事件桥接合并，
# 输出中可见 RETRACT（含 replaced_by）+ 替代 INSERT
go run ./cmd/demo -dir /tmp/swe-demo-session -session

# 模拟来源中断（退出码 2），已完成屏障均已检查点
go run ./cmd/demo -dir /tmp/swe-demo -interrupt

# 恢复续跑：从检查点继续，已确认输出不重复
go run ./cmd/demo -dir /tmp/swe-demo
```

输出会列出每条聚合结果（含 `INSERT`/`CORRECTION`/`RETRACT`、修订号、窗口、
触发水位线，撤销记录附带 `replaced_by` 替代标识）与全部旁路记录，
日志中包含窗口归属、水位线推进和判定依据（`basis=watermark>=window_end`、
`basis=session_merged`）。

---

## 9. 包结构

```
internal/
  event/       事件、异常时间与迟到策略、判定记录
  window/      滚动/滑动/会话窗口分配与确定性合并
  watermark/   单调水位线与有界乱序生成器
  aggregate/   聚合器、窗口累加状态（事件去重）、INSERT/CORRECTION/RETRACT 输出
  state/       有界状态存储（LRU、背压、落盘、确定性遍历、快照/恢复）
  checkpoint/  原子检查点（魔数+校验和、损坏检测）
  source/      可中断、可回放、带偏移量的内存事件源
  sink/        幂等内存/JSONL 文件输出 + 旁路记录
  engine/      两阶段并发编排、屏障触发、撤销发射、检查点、恢复
cmd/demo/     确定性端到端演示（含会话撤销场景 -session）
```
