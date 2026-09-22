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
- 也可以用旧订阅者 `ID` 通过 `SubscribeOption{ResumeID: id}` 在新句柄上续传。
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

## 3. 消息编码与版本兼容

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

## 4. 关键并发设计（为什么正确）

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
- **同主题序号连续**：每主题发布锁让“序号分配 + 入队”原子，
  避免多发布者并发导致的暂时缺号被误判为已确认空洞。

---

## 5. 验证方法

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
| `TestSubscriptionVisibility`  | 退订后发布立即得到 `DropNoSubscriber` |
| `TestConcurrentOverflowStress`| 慢消费者 + 队列溢出；发布方按 `KindOverflow` 重试，最终不丢不重不死锁 |
| `TestReliableOverflowAndIsolation` | 背压超时返回 `KindOverflow`，且无关主题发布不被阻塞 |
| `TestLowLatencyDropsWithReason`   | 溢出丢弃计入 `DropQueueFull`，账目守恒，发布不阻塞 |
| `TestLowLatencyNoSubscriber`      | 无订阅者给出 `DropNoSubscriber` |
| `TestCodecVersionMismatch` / `TestCodecMissingField` / `TestCodecDecodeGarbage` | 版本/字段/编码异常分别映射为可区分 Kind |
| `TestFrameMarshalRoundTrip`       | 二进制帧编解码往返、坏帧映射 `KindDecode` |
| `TestReceiveDecodeErrors`         | 订阅侧解码拒绝并返回可区分错误 |

单测通过 `t.Logf` 打印关键会话信息（收讫序号、重连续传序号、
投递/丢弃计数、溢出背压结论与错误 Kind），便于复盘每次投递判定依据。

---

## 6. 快速上手

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
