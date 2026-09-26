# Stream Window Engine (Go)

一个小型**本地流式事件聚合引擎**。用内存/本地文件模拟事件源与结果输出，
保证同一批事件在**不同到达顺序、不同并发度与重启场景**下得到一致、
可解释、可重放的结果。

- 纯 Go 标准库实现，无第三方依赖；
- 结构化日志（`log/slog`）记录每个窗口的归属、水位线推进与判定依据；
- 全部保证均有测试覆盖（乱序、迟到、重复投递、重启、内存限额、损坏检测）。

---

## 1. 窗口语义

所有窗口都是**半开区间 `[Start, End)`**：恰好落在边界上的事件归属**后一个**窗口。
窗口边界相对 epoch（或显式 `Offset`）对齐，因此窗口归属只由事件时间决定，
与到达顺序无关。

| 类型 | 配置 | 归属 |
|---|---|---|
| 滚动 Tumbling | `Size` | 事件恰好属于 1 个窗口 |
| 滑动 Sliding | `Size` + `Slide` | 事件可同时属于多个窗口（`Size/Slide` 个） |
| 会话 Session | `Gap` | 初始单事件会话 `[ts, ts+Gap)`，按事件点间隔 `<= Gap` 确定地合并 |

会话合并规则与输入顺序无关：按 `Start`（再按 `End`、`Seq`）排序后聚类，
连接判定基于**会话内最近一次事件时间**：

- 相邻两个会话的事件点间隔（后一个的最早事件 − 前一个的最晚事件）
  **恰好等于 `Gap` 时算同一个会话**；严格大于 `Gap` 时分裂为两个独立会话；
- 合并后窗口 `End = 最晚事件时间 + Gap`，`Seq` 取成员最小值（身份稳定），
  因此任何到达顺序、任何重放都得到相同会话；
- 若被合并的会话此前已发布，引擎先对其发出 `RETRACT` 再发出合并结果
  （见第 3 节）。

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

每条输出都带：

- `Seq`：引擎内全局单调递增的**投递幂等键**，sink 按 `Seq` 去重，
  重复投递（含重启重放）不会产生重复结果；
- `ResultID`：**逻辑结果的稳定标识**，只依赖数据，与运行次数、并发度、
  是否中断恢复无关（见下）；
- `Revision`：同一 `ResultID` 的修订号，首版 `1`，每次迟到修正 `+1`；
- `Kind`：`INSERT`（首次）、`CORRECTION`（迟到修正）、`RETRACT`（撤销）；
- `ReplacedBy`：仅 `RETRACT` 使用，指向取代被撤销结果的当前结果；
- `Late`、`Watermark`、`EmitAt`：解释这次输出由什么触发。

### ResultID：逻辑结果的稳定标识

| 窗口类型 | ResultID 身份 |
|---|---|
| 滚动/滑动 | 分组键 + 窗口类型 + 固定窗口边界 `[Start,End)` |
| 会话 | 分组键 + 会话内**最早事件时间**（谱系起点） |

会话被迟到事件延长末端、或与其他会话合并时，其最早事件时间不变，
因此同一会话谱系的 `INSERT → CORRECTION → （可能）RETRACT` 由同一个
`ResultID` 串起来；重放、调整并发度、从检查点续跑都不会改变它。

### RETRACT：已发布结果被撤销

当一条**此前已发布**的结果随着迟到数据到达而不再成立时（典型场景：
两个已分别发布的会话被一条迟到事件连成同一个会话），引擎在发出
替代结果**之前**先发出一条 `RETRACT`：

- `ResultID` + `Revision` 精确指向被撤销的那条已发布结果
  （`Value`/`Count`/`Window` 携带其最后发布时的快照）；
- `ReplacedBy` 指向当前真正成立的结果（存活谱系的 `ResultID`）；
- 撤销与替代结果在**同一屏障**内先后发出，顺序确定；
- 撤销按 `ResultID` 排序后发出，与并发度、到达顺序无关。

**下游对账方式**（维护「分组+窗口」增量视图）：

```
INSERT/CORRECTION:  view[ResultID] = 当前值        // upsert，按 Revision 判新旧
RETRACT:            delete(view, ResultID)         // 仅当持有版本 == 消息 Revision
```

- 幂等：重复投递由 `Seq` 去重；下游再按 `(ResultID, Revision)` 校验，
  重复撤销、重启重放、并发处理都不会重复撤销，也不会撤销仍有效的结果；
- 被撤销的 `ResultID` 不会复活：能连接到已清理会话的迟到事件必然超出
  允许迟到宽限而被丢弃（旁路记录），因此旧谱系不会被再次修正；
- 撤销元数据有界：待发撤销只挂在存活会话条目上、随触发即发出并清空，
  随检查点持久化，窗口清理（Purge）时一并消失，不随运行时间增长。

### 与既有 INSERT/CORRECTION 的关系

- 滚动/滑动窗口语义完全不变：只产生 `INSERT`/`CORRECTION`，永不产生
  `RETRACT`（固定窗口不会因迟到数据消失）；
- 会话窗口的 `INSERT`/`CORRECTION` 语义不变：迟到延长会话仍是同
  `ResultID` 的 `CORRECTION`；只有当迟到数据把**多个已发布会话合并**
  时，被并入的旧会话才收到 `RETRACT`；
- 既有配置项、输入数据格式、旁路记录格式均不变；输出 JSON 仅新增
  `result_id`/`replaced_by` 字段（旧字段含义不变），旧下游可安全忽略。

重复事件由窗口累加器按**事件 ID** 去重：同一 ID 二次进入不重复计数，
也不会造成重复修正或重复撤销（会话合并同样按事件 ID 去重）。

---

## 4. 屏障、并发与确定性

处理以**屏障批次**进行（大小 = `min(32, CheckpointEvery)`）：

1. **预处理阶段（按源顺序，串行）**：时间异常分类、观测事件时间、冻结批水位线；
2. **写入阶段（屏障内并发）**：事件按分组键 FNV 哈希分片到 `Concurrency`
   个 worker，同键事件永远在同一 worker 保序；会话合并按分组键互斥；
3. **触发阶段（屏障点）**：按 `(Group, Window.Start, End, Seq)` 确定性顺序
   统一触发窗口、分配 `Seq`。

因此：**并发执行与串行执行的输出序列逐字节一致**（有
`TestMultiGroupConcurrentEquivalence` 用乱序多键数据对比 concurrency=1/8）。

> 屏障位置只依赖输入中的绝对记录位置，与“在哪里中断/恢复”无关，
> 因此中断后续跑的输出序列与不间断运行对齐（在相同屏障调度下）。

---

## 5. 检查点与恢复

- 检查点文件 `chk.json`：魔数 + 紧凑 JSON 载荷 + **SHA-256 校验和**；
- 写入是原子的：写临时文件 → `fsync` → `rename`。中途失败不产生半成品，
  旧快照始终有效；
- 快照内容：水位线、`NextSeq`、源偏移、全部窗口累加状态（含去重 ID 集合
  及其 Value、会话最晚事件时间）、修订号、关闭/脏标记、**待发撤销队列**、
  落盘文件清单；
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
  状态占用有明确上界。

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
| 迟到合并撤销已发布结果 | `engine.TestLateMergeRetractsPublishedResult`、`TestResultLifecycleSingleLineage` |
| 会话间隔边界（==Gap 合并 / >Gap 分裂） | `engine.TestSessionGapBoundaryMergeAndSplit` |
| 级联合并撤销全部旧会话 | `engine.TestCascadeMergeRetractsAllAbsorbed` |
| 撤销序列跨并发一致 | `engine.TestRetractionSequenceConcurrencyInvariant`（-race） |
| 重启/重放后撤销不重不漏 | `engine.TestRetractionAcrossRestart`、`TestRetractionRestartAfterRetractEmitted` |
| 撤销幂等消费、合并去重 | `engine.TestRetractionIdempotentConsumption`、`TestMergedSessionLateDuplicateNotDoubleCounted` |
| 内存上限下撤销一致、元数据有界 | `engine.TestRetractionUnderMemoryLimit`、`TestRetractionMetadataBounded` |
| ResultID 规则与稳定性 | `engine.TestResultIDRules`、`TestResultIDStableForTumblingAndSliding` |
| 合并按事件 ID 去重 | `aggregate.TestAccumulatorMergeIntoDedup` |
| 滑动/会话窗口 | `engine.TestSlidingWindow`、`TestSessionWindowOrderIndependent` |
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

# 模拟来源中断（退出码 2），已完成屏障均已检查点
go run ./cmd/demo -dir /tmp/swe-demo -interrupt

# 恢复续跑：从检查点继续，已确认输出不重复
go run ./cmd/demo -dir /tmp/swe-demo
```

输出会列出每条聚合结果（含 `INSERT`/`CORRECTION`、修订号、窗口、触发水位线）
与全部旁路记录，日志中包含窗口归属、水位线推进和判定依据（`basis=watermark>=window_end`）。

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
  engine/      两阶段并发编排、屏障触发、检查点、恢复
cmd/demo/     确定性端到端演示
```
