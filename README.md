# 本地流式事件聚合引擎（Go）

一个可嵌入的本地流式聚合内核：事件按**事件时间**进入滚动/滑动/会话窗口，由**水位线**
决定触发时机；迟到事件按可配置策略处理并产生**可区分的新增/修正输出**；状态通过
**原子检查点 + 幂等输出端**实现崩溃恢复；多分组键可并行处理，输出与串行完全一致；
状态占用有明确上限，超限按背压或落盘处理。

无外部中间件依赖，事件源、检查点、输出端均为本地内存/文件实现，便于单机运行与测试。

## 目录结构

| 包 | 职责 |
|---|---|
| `event` | 事件结构、时间校验与缺失/异常时间策略（reject / assign-current-time） |
| `window` | 滚动、滑动窗口分配器（epoch 对齐、左闭右开）与会话 gap 参数 |
| `watermark` | 单调水位线生成器（`wm = max(observedEventTime) - lag`，只升不降） |
| `aggregate` | 聚合接口与 `count` / `sum` 实现（支持合并与序列化） |
| `checkpoint` | 检查点抽象、原子文件存储（tmp+fsync+rename，SHA-256 损坏检测）、内存存储 |
| `source` | 事件源抽象与内存源（可注入中断） |
| `sink` | 幂等输出端：按 `Seq` 去重的内存端与 fsync 的 JSONL 文件端 |
| `engine` | 编排核心：水位线、窗口状态、迟到策略、会话合并、背压/落盘、检查点恢复 |
| `cmd/demo` | 乱序 + 迟到 + 重启的端到端演示 |

## 核心语义

### 1. 窗口边界与事件归属（确定、可解释）

所有窗口均为 **`[Start, End)` 左闭右开**，边界对齐 Unix epoch：

- **滚动窗口** `tumbling(size)`：`Start = floor(eventTime, size)`，每个事件恰好属于一个窗口。
- **滑动窗口** `sliding(size, slide)`（要求 `slide` 整除 `size`）：一个事件恰好属于
  `size/slide` 个窗口，归属集合按开始时间升序确定。
- **会话窗口** `session(gap)`：相邻事件时间差 `> gap` 断开。引擎维护动态会话：
  新事件的候选会话 `[ts, ts+gap)` 与所有重叠现存会话形成连通簇并合并；
  **幸存者恒为簇中开始最早的窗口**（同开始取 ID 最小），因此已输出窗口的标识
  （`session[startMs,)`，只含开始时间）永不因合并/延长而改变。

**触发时机**：屏障（每次批次处理后、显式推进水位线时、关闭时）扫描，
当且仅当 `windowEnd <= watermark` 时首次输出（`reason=window-fire`）。
窗口状态在 `watermark >= end + allowedLateness` 后才清除（固定窗口）。

### 2. 异常事件时间（不静默丢弃）

事件时间缺失、早于 `MinEventTime`、或晚于 `now + FutureTolerance` 都会被判定并记录
（`Engine.InvalidEvents()`，日志含判定依据 `reason`），然后按策略处理：

- `RejectInvalidTime=true`：拒绝该事件（`ProcessBatch` 不计入接受数），绝不静默丢弃；
- `RejectInvalidTime=false`：用注入时钟重新分配事件时间后继续处理，记录仍保留。

### 3. 水位线（单调不回退）

```
watermark = max(所有已观测事件时间, 源侧进度信号) - WatermarkLag
```

- 迟到（更早）的事件时间**永远不会**使水位线回退（见 `watermark.Generator` 与其测试）；
- 源可通过 `EventBatch.Progress` / `Engine.AdvanceWatermark` 发送心跳，保证空闲源的窗口也能触发；
- 恢复后水位线只前推不回退。

**判定基准**：每个批次在准入点记录旧水位线 `wmBefore`。迟到判定
（`eventTime < wmBefore`）与本批水位线推进严格分离——同一批内先到的未触发窗口
即使伴随更晚事件，也按“首次输出”纳入（与 Flink 类引擎语义一致）。

### 4. 迟到事件策略（结果与记录可区分）

已首次输出窗口再收到 `eventTime < watermark` 的事件时：

| 策略 | 行为 | 记录动作 |
|---|---|---|
| `drop`（默认之一） | 丢弃，结果不变 | `late-drop` |
| `revise`（默认） | 允许迟到时间内计入并发出修正输出；超时丢弃 | `late-revise` / `late-drop-expired` |
| `redirect` | 进入侧输出，主结果不变 | `late-redirect`（见 `Engine.SideOutputs()`） |

允许迟到上限为 `End + AllowedLateness`；超过后窗口状态被清除。之后到达的相关事件
由**无状态判定**识别：窗口不存在且 `end + AllowedLateness <= 准入水位线` 时直接记录
`late-drop-expired`（或 redirect 侧输出），既不会被误建成新窗口，也不需要任何
无界增长的历史结构。

### 5. 新增 vs 修正输出；重复投递幂等

输出记录 `sink.Record`：

- `is_correction=false, reason=window-fire`：窗口首次输出；
- `is_correction=true, reason=late-revision`：迟到事件改变固定窗口结果；
- `is_correction=true, reason=session-revise`：迟到/合并改变会话结果；
- `is_correction=true, value=0, reason=session-retract`：会话被合并，旧窗口撤销。

幂等保证有两层：

1. **事件级**：每个窗口维护已计入的事件 ID 集合（无 ID 事件在准入时得到确定性合成 ID），
   重复投递的事件不会重复计数，也不会再次触发修正（`dirty` 仅在有新事件被实际接受时置位）；
2. **输出级**：每条输出有严格递增 `Seq`；Sink 对同一 Seq 重复 `Emit` 只生效一次
   （文件端重启时扫描历史文件重建去重集）。

### 6. 检查点与恢复（无半更新、无重复输出）

检查点为单文件信封：base64 状态 JSON + SHA-256 + 格式/版本号，
**tmp 文件 → fsync → rename → fsync 目录** 原子落盘。写入失败与损坏
（校验和/截断/格式错误）分别返回 `ErrWriteFailed` 与 `ErrCorrupt`，可区分。

检查点内容：水位线、全部 key 的窗口状态（含 fired 标记、去重集合、墓碑）、
输出游标（`nextOutputSeq` / `committedSeq`）、已持久化但未确认的 `pending` 输出、
落盘（spill）文件清单。

提交协议（“先持久化、后投递”）：

1. 屏障上把输出草稿全局排序并分配 Seq；
2. 连同遗留 `pending` 一起原子写检查点；
3. 成功后按序投递（失败则输出保留在 `pending`，返回错误；重放被 Sink 按 Seq 去重）；
4. 恢复时先以 Sink 实际 `LastSeq()` 对齐游标，再重放 `pending`。

因此：崩溃在检查点前 → 整批重放；崩溃在检查点后、投递中 → 重投但不重复；
已确认输出绝不重复发出；未完成窗口随状态快照保留。

### 7. 并发确定性

- Key 经 FNV 哈希映射到固定数量分片（`ShardCount`），分片内严格按批次顺序处理；
- 批次在全局串行点准入（校验、迟到标记、推进水位线），随后各分片**并行**做状态计算；
- 屏障再次全局串行：触发结果按 `(windowEnd, key, windowID, reason)` 统一排序后分配 Seq。

因此**并行处理的可观察输出与按输入顺序的串行处理逐字节一致**；重放同一输入得到
相同输出序列（测试以 1/2/4/16 分片、400 事件乱序压力验证）。

### 8. 状态上限：背压或落盘

- `MaxWindows` 为内存中活动窗口总量上限（按分片均摊，`ceil(MaxWindows/n)`），
  到期清除的窗口不保留任何历史结构（过期迟到事件用无状态判定拒绝），因此状态严格有界；
- `OnOverflow=block`：批次开始前容量预判超限即返回 `ErrStateLimit`，
  预判发生在任何状态变更之前，调用方可稍后重试（无半更新）；
- `OnOverflow=spill`：把最早结束（同结束按 key、windowID 排序）的未修订窗口
  原子写盘（`SpillDir`），事件再到该窗口时惰性加载；spill 文件引用进入检查点，
  孤儿文件在恢复时清理；

### 9. 失败检测（原因可区分）

| 失败 | 哨兵错误 |
|---|---|
| 来源中断 | `engine.ErrSourceFailed`（包装 `source.ErrSourceInterrupted`） |
| 检查点写入失败 | `engine.ErrCheckpointFailed`（`checkpoint.ErrWriteFailed`） |
| 检查点损坏 | `engine.ErrCheckpointCorrupt`（`checkpoint.ErrCorrupt`） |
| 输出端不可用 | `engine.ErrSinkFailed`（输出保留 pending，可重试） |
| 状态上限背压 | `engine.ErrStateLimit` |
| 时间策略拒绝 | `engine.ErrInvalidEventTime`（仅单事件 API 返回，批次中记录） |

任何写入失败都不会覆盖上一个完好的检查点；落盘失败使引擎进入 `failed` 态，
须从最近检查点恢复，避免在半更新状态上继续。

## 配置项（`engine.Config`）

| 字段 | 含义 | 默认 |
|---|---|---|
| `WindowType` | `tumbling` / `sliding` / `session` | tumbling |
| `WindowSize` / `Slide` | 滚动/滑动窗口长度、滑动步长（步长整除长度） | 10s / — |
| `SessionGap` | 会话间隔 | — |
| `Aggregate` | `sum` / `count` | sum |
| `WatermarkLag` | 水位线滞后观察最大事件时间的时长 | 1s |
| `AllowedLateness` | 窗口结束后仍接受修订的时长 | 5s |
| `LateEventPolicy` | `drop` / `revise` / `redirect` | revise |
| `RejectInvalidTime` | 异常时间拒绝（true）或重分配（false） | false |
| `MinEventTime` / `FutureTolerance` | 时间合法区间 | 不检查 / 不检查 |
| `MaxWindows` | 内存窗口上限 | 1000 |
| `OnOverflow` | `block` / `spill` | block |
| `SpillDir` / `CheckpointDir` | 落盘与检查点目录 | — |
| `CheckpointEveryN` | 运行循环每 N 事件显式检查点（0=仅屏障时） | 0 |
| `ShardCount` | 并发分片数 | 1 |

## 日志

默认使用 `log/slog` 结构化日志，关键判定均有记录，例如：
`window fired`（含 `window_id/end_ms/watermark_ms/basis`）、
`late event accepted for revision` / `late event dropped`（含水位线、事件时间、动作、依据）、
`window correction emitted`、`session windows merged`、`checkpoint saving`、
`window spilled to disk`、`state limit reached`。可用 `Options.Logger` 替换。

## 验证方法

```bash
# 全量测试（含 -race 竞态检测）
CGO_ENABLED=1 go test -race ./...

# 仅竞态压测（乱序/多分片串行等价）
go test ./engine/ -run TestParallelApplicationMatchesSerial -v

# 端到端演示（乱序 → 迟到修正 → 重启恢复 → 再次修正）
go run ./cmd/demo --dir .swe-demo
cat .swe-demo/out.jsonl
```

测试覆盖要点：

- **乱序**：同批/跨批乱序事件的首次输出与修正（`determinism_test.go`）；
- **迟到**：drop / revise / redirect 三策略、allowedLateness 过期、墓碑判定（`late_test.go`）；
- **重复投递**：相同事件 ID 不重复计数、不重复修正；Sink 同 Seq 幂等（`late_test.go`、`sink`）；
- **重启**：水位线/未完成窗口/已确认输出恢复、pending 重放不重复、落盘窗口跨重启修订（`checkpoint_test.go`、`spill_test.go`）；
- **内存限额**：背压返回可区分错误、溢出落盘+惰性加载结果正确（`limits_test.go`）；
- **会话**：gap 分窗、迟到桥接产生撤销+修正（`windows_test.go`）；
- **损坏/中断**：检查点篡改被检出、写入失败被包装、源中断可区分（`checkpoint_test.go`、`limits_test.go`）。
