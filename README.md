# robotics-pubsub-go

本地可运行的机器人消息总线（进程内实现，无外部依赖）。用单进程内的“发布漏斗 + 订阅会话”
模拟多个发布者与订阅者之间的消息传递，目标是让**不同服务质量与订阅状态下的投递结论一致、
可解释、可复现**。

- 纯 Go 标准库，无第三方依赖；`go run ./cmd/demo` 即可端到端演示。
- 每个主题一个发布漏斗 goroutine，把该主题上的所有发布线性化：分配单调序号、取一致的
  订阅者快照、按各订阅的 QoS 裁决，因此**同主题顺序天然保持**。
- 背压按订阅隔离：某主题/某订阅的拥塞只阻塞对应高可靠发布者，不影响其他主题，也不存在
  总线级锁上的等待，无死锁。

---

## 1. 主题与订阅模型

### 1.1 主题（topic）与序号

- 主题是字符串，按需创建；每个主题维护一个从 1 开始、严格单调递增的 `Seq`。
- 一次发布在**主题漏斗**中原子完成三件事：分配 `Seq`、复制当前订阅者快照、对快照中的
  每个订阅执行入队裁决。因此“谁能收到这条消息”对所有参与者语义一致：
  发布返回的 `PublishReport.Outcomes` 即当时每个订阅的确定结论。

### 1.2 订阅会话（session）与续传

- 订阅身份是 `(clientID, topic)`。`Subscribe` 建立会话，`Detach` 短暂离线但**保留会话**，
  再次用相同 `clientID` 调用 `Subscribe` 即恢复同一会话。
- 会话状态：
  - 已 `Ack` 的消息立即从会话移除，**永不再投**；
  - 已派发但未裁决（未 Ack/Nack、或回调执行中断连）的高可靠消息，重连后以
    `Redelivered=true`、`Attempt` 递增的方式**按序重投**；
  - 离线期间到达的高可靠消息进入会话队列，重连后按序投递，**不静默丢失**。
- `Unsubscribe(clientID, topic, deleteSession)`：
  - `deleteSession=false` 等价于 `Detach`（保活待续传）；
  - `true` 彻底删除会话与队列，之后同 clientID 再订阅是全新会话。
- 订阅关系的增、删、改、重连都会递增两个版本号，可用于断言“变更立即可见”：
  - 主题级 `TopicEpoch`：该主题订阅关系版本；
  - 会话级 `Subscription.Epoch()`：该会话的连接/QoS 变更版本。

### 1.3 投递与裁决（Delivery）

- 每个高可靠订阅**同一时刻只有一个在途投递**：上一条未裁决，不派发下一条，保证严格顺序。
- `Delivery.Ack()` / `Nack()` 至多生效一次（内部 `sync.Once`）。
- 每次投递尝试带一个单调**令牌（token）**。断连会作废旧令牌；旧 handler 即使迟到调用
  Ack/Nack，也无法影响重连后的新尝试——避免“旧确认误删新投递”，从而杜绝重复/丢失。

---

## 2. 服务质量（QoS）语义

| 等级 | 投递保证 | 顺序 | 满时行为 | 适用 |
|------|----------|------|----------|------|
| `QoSReliable(1)` | 至少一次，未确认必重投，已确认不重复 | 同主题严格顺序 | 发布者**阻塞背压**直到有空位或 ctx 超时 | 位姿、指令、关键事件 |
| `QoSLowLatency(0)` | 尽力而为，允许丢弃 | 入队/派发保序 | **绝不阻塞**，立即返回可区分丢弃原因 | 高频遥测、可过期采样 |

### 2.1 高可靠（reliable）

- 顺序：订阅派发循环单在途，前一条 Ack/Nack 结算后才派下一条；Nack 的消息在队首重试，
  后续消息不会越过它。
- 至少一次：断连、handler panic（自动按 Nack 处理）、显式 Nack 都会重投。
- 背压：队列容量由 `WithQueueDepth` 限定（含 1 个在途）。满时发布者等待，可用
  `context.WithTimeout` 控制等待上限，超时返回 `KindBackpressure` 错误。
- 隔离性：等待发生在**该订阅自己的条件变量**上，不持有任何总线/主题锁，
  因此不会阻塞其他主题或其他订阅的发布者。

### 2.2 低延迟（low-latency）

- 容量占用 = 排队消息数 + 在途回调数（`WithMaxLowLatencyInflight`，默认等于队列深度）。
  慢消费者会真实占满容量，从而触发丢弃，但**发布调用永不被慢消费者阻塞**。
- 丢弃必须给出可区分原因（`DropReason`），并体现在 `PublishReport.Outcomes[].Reason`
  与会话统计 `Stats().DroppedByReason`：
  - `DropQueueFull`：有界容量（队列+在途）已满；
  - `DropSubscriberOffline`：会话保留但当前未连接；
  - `DropRateLimited`：触发 `WithLowLatencyRateLimit` 令牌限速；
  - `DropSubscriptionClosed`：订阅关系已删除。
- 满时策略 `WithDropPolicy`：
  - `DropNewest`（默认）：丢弃新到消息；
  - `DropOldest`：淘汰队列中最早的**可丢弃低延迟消息**（在途高可靠消息绝不被淘汰）。

---

## 3. 消息编码与版本兼容

- 信封 `Envelope` 携带 `Schema`（逻辑 schema 名）、`Version`（编码版本）、`Encoding`
  （当前仅 `json`）与负载 `Data`；总线发布时补齐 `ID` 与按主题单调的 `Seq`。
- `SchemaRegistry` 为每个 schema 注册：当前可生产版本 `currentVersion`、仍可接受的最旧
  版本 `minVersion`、以及一个解码/迁移函数。
- 兼容规则：
  - `version < min` 或 `version > current`：明确拒绝，错误 `KindVersionMismatch`；
  - 窗口内的旧版本：由迁移函数升级为当前结构（示例把 `robot.pose` V1 迁移到 V2，
    补 `frame="map"`、`confidence=1.0` 默认值）；
  - schema 未注册：`KindUnknownSchema`；
  - JSON 语法错误 / 空负载 / 不支持的 encoding：`KindDecode`；
  - 必填字段缺失或为 null：`KindMissingField`（用 `RequireFields` 声明）；
  - 编码失败：`KindEncode`。
- **底层异常不直接透出**：所有错误统一为 `*pubsub.Error`，对外只暴露 `Kind/Op/Topic/Detail`，
  底层 cause 仅保存在 `Err` 中供 `errors.As/Is` 与日志使用。

辅助函数：`MigrateJSON(env, migrate)` 先解成 `map[string]json.RawMessage` 再交给版本迁移
回调；`RequireFields(env, raw, fields...)` 做必填校验。

---

## 4. 错误类型一览（可区分、可 errors.As）

`KindEncode` / `KindDecode` / `KindMissingField` / `KindVersionMismatch` /
`KindUnknownSchema` / `KindBackpressure` / `KindClosed` / `KindConflict` /
`KindInvalidArgument`。另有哨兵：`ErrQueueFull`、`ErrBusClosed`、`ErrSubNotFound`、
`ErrAlreadyExists`、`ErrVersionMismatch`。

---

## 5. 验证方法

```bash
# 构建
go build ./...

# 静态检查
go vet ./...

# 全部单测
go test ./...

# 并发/竞态检测（发布并发、重连 churn、队列溢出、背压隔离都在此覆盖）
go test ./... -race

# 高压复现（可调大 count）
go test . -race -count=100

# 端到端演示：顺序投递、离线重连续传、QoS 丢弃原因、版本迁移/拒绝
go run ./cmd/demo
```

### 5.1 测试覆盖

- `codec_test.go`：V1→V2 迁移；未知 schema、版本越界、坏 JSON、缺字段、坏 encoding
  各自映射到可区分的错误类型；编码版本号正确。
- `qos_test.go`：
  - 高可靠 50 条严格顺序且每条恰好投递一次；
  - Nack 后在队首重投，后续消息不插队；
  - 离线/重连：已确认的 seq1 不重复，在途的 seq2 重投，离线期 seq3 不丢，按序结算。
- `backpressure_test.go`：
  - 低延迟队列满 → `DropQueueFull`；离线 → `DropSubscriberOffline`；限速 → `DropRateLimited`；
  - 慢消费者占满“在途+队列”后，后续发布立即得到丢弃且**发布者不被阻塞**；
  - 主题 A 持续背压时，主题 B 的发布完全不受影响；
  - 背压等待可被 ctx 超时打断，返回 `KindBackpressure`。
- `concurrent_test.go`：8 发布者 × 200 消息并发下顺序 1..N 无重复；持续发布中反复
  detach/resume（churn）零丢失、重投账目自洽；订阅增删对后续发布立即可见。
- `concurrency_edge_test.go`：发布与删除订阅高并发（2 万次）不 panic/不死锁；
  QoS 升级对“之后”的消息立即可见；重复删除返回可区分错误。

### 5.2 日志（关键会话与投递判定依据）

总线日志带 `pubsub:` 前缀，打印判定依据，便于解释与复现：

- `[relation]` 订阅建立/恢复/删除及 `topicEpoch`；
- `[session]` connected/resumed、detached、terminated 及 `epoch/pending`；
- `[publish]` 每条消息的 `seq/schema 版本/recipients/dropped/topicEpoch`；
- `[deliver]` 每次投递的 `qos/attempt/redelivered`；
- `[verdict]` acked（不再投递）/ nacked（队首重试）；
- `[redeliver-pending]` / `[verdict-ignored]` 在途尝试因断连作废、旧裁决被忽略；
- `[drop]` / `[backpressure]` 丢弃原因与队列水位。

单测使用捕获型 logger，同时输出到 `t.Log`（`go test -v` 可见）并在失败时转储完整判定日志。
