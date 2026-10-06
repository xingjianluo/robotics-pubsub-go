# robotics-pubsub-go

一套**本地可运行**的机器人消息总线：用进程内回环（in-process loopback）模拟多个
发布者与订阅者之间的消息传递，目标是让不同服务质量（QoS）与订阅状态下的
投递结论**一致、可解释、可复现**。

无外部中间件依赖，仅用 Go 标准库；所有正确性结论都有单测与
`go test -race` 覆盖。

---

## 1. 主题与订阅模型

### 主题（Topic）

- 主题是字符串路由键（如 `robot/pose`、`robot/cmd`）。
- 每条消息在其主题内被分配一个**单调递增序号 `Seq`**（从 1 开始）。
  同一主题的发布按主题锁串行完成“分配序号 → 入队”，因此订阅者看到的
  消息严格按 `Seq` 升序。
- 主题可注册一个 `Schema`，声明当前版本与必填字段，用于编解码与版本兼容。

### 订阅者（Subscription）

- `Subscribe(topic, qos, opt)` 创建订阅，返回带稳定 `ID` 的句柄。
- 每个订阅者拥有独立的**投递通道**（容量 = `QueueLen`）与**未确认存储**：
  - `pending`：所有“已入队但未 Ack”的消息，无论是否已送入通道；
  - 未 Ack 的消息永不被静默丢弃，可重发/续传；
  - 已 Ack 的消息立即从 `pending` 删除，**绝不重复投递**（Ack 幂等）。

### 订阅状态机与会话续传

```
新建(active) ──Pause──▶ offline ──Resume──▶ active ──Close──▶ removed
```

- **Pause（短暂离线）**：停止投递，但订阅关系、通道与 `pending` 全部保留。
  离线期间发布的消息直接进入 `pending` 累积。
- **Resume（重新接入）**：重建投递通道，把所有未 Ack 消息的投递状态复位，
  调度水位回退到“最小未确认序号 − 1”，随后按序号顺序重投：
  - 未确认消息会被重新投递（至少一次）；
  - 已确认消息不在 `pending` 中，不会重复。
- 也可以用旧订阅者 `ID` 通过 `SubscribeOption{ResumeID: id}` 在新句柄上续传，
  语义与 `Resume` 完全等价（同样重建通道、复位水位）；
  续传后应重新调用 `Messages()` 读取新通道。
- **续传保证**：任何时候（包括一边压测一边反复续传）都不丢消息、
  首投严格按 `Seq` 升序、已确认不重复、不会偶发超时。实现上每个订阅
  关系同一时刻只有一个调度协程，续传通过“调度代际号”作废旧代际的
  在途发送，杜绝多发送者并发乱序与水位回退竞态。
- **Close / Unsubscribe**：退订并销毁状态，之后不可再续传。
- 订阅关系的增删改在总线互斥下完成，发布时取订阅表快照，
  **对所有参与者立即可见且语义一致**（退订后再发布立即得到无订阅者结论）。

---

## 2. 服务质量（QoS）语义

发布 QoS 与订阅 QoS 取**较低者**生效（`effective = min(pubQoS, subQoS)`）。

### `QoSReliable = 1`（高可靠）

- **同一主题顺序投递**：订阅者通道内消息严格按 `Seq` 升序；
  调度只从连续水位的下一条选取，高序号不会越过尚未投递的低序号。
- **至少一次送达（at-least-once）**：未 Ack 消息保留在 `pending`：
  - 离线重连（Resume）后重投；
  - “已读走但超过 `RedeliveryTimeout` 仍未 Ack”时进入重发队列，
    在不破坏顺序的前提下重新投递，投递记录携带 `Attempt`，
    消费方可据 `Seq`/`Attempt` 去重。
- **背压有界、不静默丢失、不连累无关发布者**：
  - 每个订阅者有独立窗口；窗口满时发布仅做有界等待
    （受 `ctx` 与 `ReliableEnqueueTimeoutMs` 约束）；
  - 超时返回 `KindOverflow` 错误，**已入队的消息仍保留**，发布方可重试；
  - 背压等待按“每主题锁 + 每订阅者窗口”隔离，慢订阅者不会阻塞
    **其他主题**的发布者（有界超时也保证不会永久死锁）。

### `QoSLowLatency = 0`（低延迟）

- **绝不阻塞**发布者：在线时尽力把消息直接送入订阅者通道。
- 允许丢弃，但每次丢弃都在 `PublishResult.Dropped` 中给出
  **可区分的丢弃原因**：

| DropReason          | 含义                       |
| ------------------- | -------------------------- |
| `DropQueueFull`     | 订阅者队列溢出（慢消费者） |
| `DropNoSubscriber`  | 该主题当前无订阅者         |
| `DropEncodeFailed`  | 编码失败                   |
| `DropBusClosed`     | 总线已关闭                 |

- 订阅者处于**离线**状态时即使是低延迟等级，消息也会进入 `pending`
  保留以便续传（不静默丢失）；丢弃只发生在“在线但跟不上”的溢出场景。

### 投递判定依据（可解释）

- `PublishResult{Topic, Seq, Delivered, Dropped[]}`：本次发布对每个订阅者
  的入队/丢弃结论。
- `Delivery{Msg, Attempt}`：投递时的消息与第几次尝试。
- 错误统一为 `*pubsub.Error{Kind, Topic, Reason, Err}`，调用方用
  `pubsub.IsError(err, kind)` 判定，底层异常被包装不透出。

---

## 3. 执行器组（同一主题多执行器分担）

执行器组（`JoinGroup`）让同一主题下挂多个执行器一起分担任务：
组内每条可靠消息**任一时刻只由一个成员接手**，同一顺序键（`Key`，
如机器人 ID / 任务 ID）的消息**严格按发布顺序**投递。

```go
worker, _ := bus.JoinGroup(ctx, "robot/cmd", "executors", pubsub.GroupOption{
    QueueLen: 16, MaxPending: 256, DeliveryTimeoutMs: 5000, MaxAttempts: 5,
})
res, _ := bus.PublishKeyed(ctx, "robot/cmd", "robot-7", pubsub.QoSReliable, cmd)
d := <-worker.Messages()   // 组内只有一个成员会收到这条
_ = worker.Ack(d.Msg.Seq)  // 确认后绝不再派
```

### 分担与故障转移

- **独占接手**：键内队首（head-of-line）投递——同一 Key 任一时刻只有
  队首一条在途，确认后才放行下一条；投递前键被绑定到某一在线成员，
  不会同一条消息同时派给两个成员。
- **成员掉线（Leave）**：其名下键的未确认消息立即变为可重分配，
  由调度器转给仍在线的成员（`Attempt` 递增的重投，消费方按 `Seq`
  去重）；没有在线成员时消息留在组内积压等待，**不静默丢失**，
  也不会堵住该主题的发布（积压在 `MaxPending` 之内发布照常成功）。
- **处理到一半离开再回来**：成员状态保留，用
  `JoinGroup(..., GroupOption{MemberID: id})` 重新入组即续传；
  已确认的消息不会重复，未确认的从最小序号接着按序投递。
- **成员变更下的顺序**：顺序键按哈希绑定在线成员，成员变化只迁移
  “待重新分配”的键；无论换没换执行器，同键消息始终按主题序号升序到达。

### 顺序与续传分别保证到什么程度

- **首投顺序（严格）**：同一 Key 的消息首次投递严格按主题序号升序；
  键内队首语义保证高序号不会越过未确认的低序号被首次投递。
- **重投（至少一次，可乱序到达）**：故障转移/死信重投产生的是
  “重试副本”，可能晚于同键更新的消息到达（首投顺序不受其影响）；
  消费方按 `Seq`（必要时结合 `Attempt`）去重即可。
- **接手（独占）**：任一时刻同一条消息只派给一个成员；
  每条消息最终被确认恰好一次，确认后绝不再派。
- **续传（不丢）**：成员离线期间的消息在组内积压保留，
  重新入组后从最小未确认序号接着按序投递；已确认不重复。

### 容量上限与失败语义（彼此可区分）

| 场景 | 对外结论 |
| ---- | -------- |
| 可靠发布时组积压达到 `MaxPending` | 有界等待后返回 `KindOverflow`，已入队消息保留；**被拒收的这条消息落入组死信队列（`DeadBacklogOverflow`）** |
| 低延迟发布时组积压满 | 不阻塞，`PublishResult.Dropped` 给出 `DropQueueFull`；**被丢弃的这条消息同样落入组死信队列（`DeadBacklogOverflow`）** |
| 消息入组超过 `DeliveryTimeoutMs` 未确认 | 判死进死信队列，原因 `DeadTimeout` |
| 投递尝试超过 `MaxAttempts` | 判死进死信队列，原因 `DeadMaxAttempts` |

- 即：**所有“最终没投成功”的消息**（积压超限被拒收、等待处理超时、
  尝试次数耗尽）都统一落到组死信队列，可查看、可重新投递，
  不会出现“拿到了失败结论但消息本身没了”的情况。
- 背压等待只阻塞本主题本次发布，**不拖住其他主题的发布者**；
  调度器对单个成员通道的发送有上限（成员离线立即解除），
  慢成员不会把全组永久卡死。
- 对外只暴露 `*pubsub.Error`（按 `Kind` 判定）与 `PublishResult`，
  内部异常不直接冒到调用方。

### 死信队列（可查看、可重新投递）

```go
dead, _ := bus.DeadLetters("robot/cmd", "executors") // 查看
_ = bus.RedeliverDeadLetter("robot/cmd", "executors", dead[0].Seq) // 重新投递
```

- 最终未能投递成功的消息进入组级死信队列，携带 `Reason`/`Attempts`/
  原序号与负载，四种原因（`DeadTimeout`/`DeadMaxAttempts`/
  `DeadBacklogOverflow`）彼此可区分。
- 重新投递以**原序号**插回其键队列的升序位置（顺序保持），
  `Attempt` 重新计数；已确认/未判死的消息不会被重复注入，
  同一序号也不会重复落信。
- 重投的消息**不会被同键在途/排队的消息卡死**：在途消息按序号
  （而非队列位置）跟踪，前序消息被确认/判死后，重投消息立即
  按序放行，不会再次被误判超时。

---

## 4. 消息编码与版本兼容

两层编码：

1. **负载层 `Codec`**（默认 `JSONCodec`）：
   业务对象编码为带版本包装的 JSON：`{"_v": <version>, "p": <payload>}`。
2. **帧层 `frameCodec`**（内部回环/链路用）：固定头二进制帧
   `magic "RPS1" | version u16 | seq u64 | topicLen u16 | idLen u16 | topic | id | body`。

### 版本与字段规则

- 通过 `JSONCodec.Allow(topic, versions...)` 声明主题**可接受的版本集合**。
- 订阅侧 `Receive` 解码时把异常映射为**可区分的错误类型**：

| 场景               | 错误 Kind             |
| ------------------ | --------------------- |
| 负载不是合法 JSON / 帧异常 | `KindDecode`        |
| 必填字段缺失或为 null      | `KindMissingField`  |
| 版本不在可接受集合         | `KindVersionMismatch` |
| 队列溢出 / 背压超时        | `KindOverflow`       |
| 可靠发布但无订阅者         | `KindNoSubscriber`   |
| 总线已关闭                 | `KindClosed`         |
| 参数非法                   | `KindInvalidArgument` |

- 业务代码只应依赖 `Kind` 做分支；底层错误（如 `json.SyntaxError`）
  被包在 `Error.Err` 中，不直接透出到对外接口。
- 兼容原则：**能解析就明确解析，不能解析就按上表明确拒绝**，
  不会出现“静默按错误版本解读”。

---

## 5. 关键并发设计（为什么正确）

- **每订阅者单投递协程**：顺序由单协程 + 连续调度水位
  `scheduledSeq` 保证；候选严格为水位之后第一个“未确认且不在通道”的消息。
- **未确认集合 `pending` 与调度水位解耦**：Ack 只删除 `pending`，
  不影响单调的调度水位，因此已确认消息不重发、未确认消息不丢失。
- **通道满不死锁**：投递协程在锁外做阻塞发送，满时只是挂起且不持锁，
  消费者可自由读走；不依赖消费者回发“读槽”信号（直接读 `Messages()`
  通道也正确）。
- **不丢唤醒**：等待点用“持锁安装等待通道 → 解锁等待其被 close”的
  channel-Cond 模式实现（`signalLocked` 在同一把锁内 close 当前等待通道），
  消除了条件变量的丢信号窗口。
- **关通道不与发送竞态**：投递协程在锁内登记在途发送（`sendWg`，
  按通道代际区分），resume/关闭先通过 `abortCh`/`leaveCh` 解除在途
  阻塞发送、等**旧代际**归零，再关闭旧通道；新代际的发送在另一个
  WaitGroup 上计数，与旧代际的等待互不干扰，消除 close/send data race
  与 WaitGroup 误用。
- **续传不丢不乱**：每个订阅关系同一时刻只有一个调度协程
  （`loopRunning` 去重，Pause 不杀协程、续传不重复起协程）；
  续传重置（`resetForResume`）自增调度代际号 `epoch`，旧代际在途
  发送醒来后不得回滚调度水位，水位只能由新一代重排——
  杜绝“回滚把水位退到未投递消息之后”的丢消息竞态。
- **同主题序号连续**：每主题发布锁让“序号分配 + 入队”原子，
  避免多发布者并发导致的暂时缺号被误判为已确认空洞。
- **组调度单协程 + 键内队首**：每执行器组一个调度协程，锁内决策、
  锁外发送；发送可被成员离线（`leaveCh`）或限时等待解除，
  关闭成员通道前等在途发送归零（`sendWg`），消除 close/send 竞态。

---

## 6. 验证方法

```bash
# 全部单测
go test ./...

# 竞态检测（核心场景要求零 data race）
go test -race ./...

# 压力复现（并发/重连/溢出场景反复运行）
go test -race -count=40 ./...
go test -count=100 ./...

# 可运行示例
go test -run Example -v
```

### 测试覆盖

| 测试                          | 场景与判定依据 |
| ----------------------------- | -------------- |
| `TestConcurrentPublishOrder`  | 8 发布者 × 60 条并发；严格升序、无重复、无 data race，打印会话统计 |
| `TestReconnectNoLossNoDuplicate` | 离线期间发布 100 条，重连后恰好收到 51..150，已 Ack 的 1..50 不重投 |
| `TestResumeByID`              | 用订阅者 ID 在新句柄上续传 |
| `TestResumeStressNoLoss`      | 边发布边反复离线-续传（交替 Resume / ResumeID 两条路径，43 轮）：300 条全收、首投严格升序、已确认不重复、不超时 |
| `TestSubscriptionVisibility`  | 退订后发布立即得到 `DropNoSubscriber` |
| `TestConcurrentOverflowStress`| 慢消费者 + 队列溢出；发布方按 `KindOverflow` 重试，最终不丢不重不死锁 |
| `TestReliableOverflowAndIsolation` | 背压超时返回 `KindOverflow`，且无关主题发布不被阻塞 |
| `TestLowLatencyDropsWithReason`   | 溢出丢弃计入 `DropQueueFull`，账目守恒，发布不阻塞 |
| `TestLowLatencyNoSubscriber`      | 无订阅者给出 `DropNoSubscriber` |
| `TestCodecVersionMismatch` / `TestCodecMissingField` / `TestCodecDecodeGarbage` | 版本/字段/编码异常分别映射为可区分 Kind |
| `TestFrameMarshalRoundTrip`       | 二进制帧编解码往返、坏帧映射 `KindDecode` |
| `TestReceiveDecodeErrors`         | 订阅侧解码拒绝并返回可区分错误 |
| `TestGroupSharedConsumption`      | 3 执行器分担 60 条：每条恰好一个成员接手一次、同键严格升序、负载确实分摊 |
| `TestGroupMemberFailover`         | 成员持未确认消息掉线：已确认不重投，未确认转派在线成员且键内顺序不乱 |
| `TestGroupRejoinResume`           | 全员离线时发布照常积压；原成员凭 ID 回来按序续传、已确认不重复 |
| `TestGroupBacklogOverflow`        | 组积压超限返回 `KindOverflow`；低延迟发布给 `DropQueueFull`；其他主题不受拖累 |
| `TestGroupBacklogOverflowDeadLetter` | 积压超限/等待超时拒收的消息（可靠 `KindOverflow` + 低延迟 `DropQueueFull`）落死信（`DeadBacklogOverflow`），可查看、按序重投、不重复 |
| `TestGroupDeadLetterTimeoutAndRedeliver` | 超时未确认进死信（`DeadTimeout`）；重投保序、计数重置、判错可区分 |
| `TestGroupRedeliverWhileKeyBusy`  | 同键有在途+排队消息时重投死信：确认前序后重投立即放行、键内原序、计数重置、不被卡死不再误判超时 |
| `TestGroupMaxAttempts`            | 反复“收到不确认就掉线”耗尽尝试次数，判死原因 `DeadMaxAttempts` |
| `TestGroupConcurrentChurn`        | 3 成员频繁上下线 + 4 发布者 × 50 条：200 条恰好确认一次、同键有序、其他主题不阻塞、`-race` 零竞态 |

单测通过 `t.Logf` 打印关键会话信息（收讫序号、重连续传序号、
投递/丢弃计数、溢出背压结论与错误 Kind），便于复盘每次投递判定依据。

---

## 7. 快速上手

```go
bus := pubsub.New(pubsub.BusConfig{DefaultQueueLen: 16}, nil)
defer bus.Close()
bus.RegisterSchema(mySchema{})

sub, _ := bus.Subscribe(ctx, "robot/cmd", pubsub.QoSReliable,
    pubsub.SubscribeOption{QueueLen: 16})

res, err := bus.Publish(ctx, "robot/cmd", pubsub.QoSReliable, payload)
// 可靠：err != nil（如 KindOverflow）时可安全重试，已入队消息不丢

msg, value, err := sub.Receive(ctx) // 按 schema 解码，错误 Kind 可区分
_ = sub.Ack(msg.Seq)                // 确认，保证不重复投递
```

### 配置项 `BusConfig`

| 字段                       | 含义 |
| -------------------------- | ---- |
| `DefaultQueueLen`          | 订阅者投递通道默认容量/在途窗口 |
| `ReliableEnqueueTimeoutMs` | 可靠发布遇慢订阅者背压的有界等待上限 |
| `RedeliveryTimeoutMs`      | 已投递未 Ack 消息的重发间隔（会话续传） |
| `GroupMaxPending`          | 执行器组默认积压上限（组内未确认消息总数） |
| `GroupDeliveryTimeoutMs`   | 组内消息默认投递超时，超时判死进死信队列 |
| `GroupMaxAttempts`         | 组内消息默认最大投递尝试次数，超过判死 |
