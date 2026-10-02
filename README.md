# robotics-pubsub-go

一套**本地可运行**的机器人消息总线：用进程内回环（in-process loopback）模拟多个
发布者与订阅者之间的消息传递，目标是让不同服务质量（QoS）与订阅状态下的
投递结论**一致、可解释、可复现**。

支持两类消费模型：

1. **独立订阅**：每个订阅者收到全量副本（广播），带两级 QoS、会话续传；
2. **多执行器分担组（Executor Group）**：同一主题下多个执行器竞争消费，
   一条消息同一时刻只交给一个执行器；掉线自动转接/回来续做；按
   “机器人 ID / 任务 ID”做同 Key 严格有序；积压超限与等待/处理超时
   分别给出可区分失败结果并进入可查看、可重投的死信队列。

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

## 3. 多执行器分担（Executor Group / Competing Executors）

面向真实机器人调度的用法：**同一主题的同一个分担组下同时挂多个执行器**，
一起分担该主题上的任务。

```go
g, _   := bus.CreateExecutorGroup(ctx, "robot/cmd", "line-A", pubsub.GroupConfig{
    MaxBacklog:       256,  // 组内保留（排队 + 在途未 Ack）上限
    ExecutorQueueLen: 16,   // 每个执行器的在途窗口
    ProcessTimeoutMs: 5000, // 接手后处理超时
    WaitTimeoutMs:    30000,// 排队等待接手超时
    MaxAttempts:      3,    // 最多派发次数（含转接）
})
ex, _  := g.Join(ctx, pubsub.ExecutorOption{})             // 执行器上线
res, _ := bus.PublishToGroup(ctx, "robot/cmd", "line-A",
    pubsub.QoSReliable, payload, &pubsub.ExecutorGroupKey{Key: "robot-7"})
d := <-ex.Messages()
_ = ex.Ack(d.Msg.Seq)
_ = ex.Leave(ctx)                                          // 执行器掉线
ex2, _ := g.Join(ctx, pubsub.ExecutorOption{ResumeID: ex.ID()}) // 回来续做
```

### 3.1 竞争消费：一条消息同时刻只属于一个执行器

- 组是一个 **competing consumers** 域：每条消息同一时刻只派发给**唯一一个**
  执行器，不会被几个执行器同时拿走（`TestExecCompetingConsumers` 用
  “seq -> 唯一属主”表断言）。
- 多个在线执行器按 round-robin 公平分担；执行器有独立的在途窗口
  （`ExecutorQueueLen`），派发只在有空槽时发生。
- 派发是组内单协程完成的，且“**选活 → 打租约 → 入通道**”在同一把组锁的
  临界区内完成（槽位由在途计数保证，入通道不阻塞），因此从根上不存在
  “租约已打、执行器却已离开”的窗口，也就不会同一条被两人同时拿到。

### 3.2 掉线切换与会话续传保证到什么程度

执行器生命周期与订阅者类似：
`Join(active) ──Leave──▶ offline ──Join(ResumeID)──▶ active ──Close──▶ removed`

- **Leave（掉线）**：立即释放它持有的全部未 Ack 租约。组内还有其他在线
  执行器时，这些活会**立即转接**给在线者（投递记录 `Attempt` 递增）；
  没有在线者时消息**原样保留在组内**，等它（或后来者）上线继续，
  主题不会被堵死。
- **Join(ResumeID)（回来续做）**：以相同身份重新接入并重建接收通道；
  掉线期间新发布到组里的消息与它未完成的消息都会继续派发。
- **保证边界**：
  - 未 Ack 的消息**不会静默丢失**（保留在组内直到 Ack / 进入死信）；
  - 已 Ack 的消息立即从组内保留集合删除，**绝不再次派发**；
  - 掉线转接属于 at-least-once：若执行器在 Ack 与租约释放的竞态中实际
    已完成但确认迟到，转接副本可能再次出现，消费方需按 `Msg.Seq` 幂等。
- Close 是永久退出（身份不可续），未 Ack 消息同样转接/保留。

### 3.3 顺序保证：同一机器人/同一任务（同 Key）严格有序

- 发布时可用 `*ExecutorGroupKey{Key: robotOrTaskID}` 声明顺序键
  （`PublishRawToGroup` 直接传 key 字符串）；未声明归入默认键。
- **同一个 Key 的消息严格按发布先后处理**：每个 Key 维护一条有序队列，
  只要队头消息还在某个执行器手上（未 Ack），同 Key 的后续消息一律等待，
  因此即使中途换执行器、成员数量变化，顺序也不会被打乱。
- **不同 Key 之间允许并行**分担（不同机器人/任务互不阻塞）。
- 等待超时/转接的副本只针对当前队头，重发不会插到同 Key 更新的消息之前。
- 注意保证范围是**每个 Key 内**；不声明 Key 或不同 Key 之间不保证全局顺序
  （那本来就是为了并行分担）。

### 3.4 容量上限与失败语义（三类结果彼此可区分）

组发布**从不为“等执行器处理”而阻塞发布者**，积压与超时都有明确归宿：

| 情形 | 可靠发布 (`QoSReliable`) | 低延迟发布 (`QoSLowLatency`) | 消息归宿 |
| ---- | ------------------------ | ----------------------------- | -------- |
| 组内积压达到 `MaxBacklog` | 返回 `KindOverflow` | 不阻塞，`Dropped=[{DropQueueFull}]` | 死信 `DeadBacklogOverflow` |
| 排队等待接手超过 `WaitTimeoutMs`（始终无执行者接手） | 死信 `DeadWaitTimeout` | 同左 | 死信 `DeadWaitTimeout` |
| 接手后超过 `ProcessTimeoutMs` 未 Ack | 先转接重派；达到 `MaxAttempts` 仍未 Ack → 死信 | 同左（组内派发始终 at-least-once） | 死信 `DeadProcessTimeout` |
| 目标组不存在 | 返回 `KindNoSubscriber` | `Dropped=[{DropNoSubscriber}]` | 不保留（无处可去） |

- 三种死信原因 `DeadBacklogOverflow` / `DeadWaitTimeout` /
  `DeadProcessTimeout` 彼此可区分，死信记录还带 `Attempts`、`Key`、
  `DeadAt`，可解释每条消息为什么最终没成功。
- 对外只暴露总线自己的结果类型（`*pubsub.Error` + `Kind`、
  `PublishResult` + `DropReason`、`DeadLetter` + `DeadReason`）；
  内部通道关闭等异常均被 recover/包装，不直接冒到调用方。
- 慢/堆积的组不会拖住其他主题：序号分配与入队仍走“每主题锁”，组满时
  立即失败入死信，不做无限等待（`TestExecGroupTopicIsolation` 断言无关
  主题发布在微秒级返回）。

### 3.5 死信队列：可查看、可重新投递

- `g.DeadLetters()` 查看死信快照（不清空）。
- `g.RequeueDead(ctx, seq)` 把死信按**原 Key** 投回：重置尝试次数，并插到
  该 Key 队列**最前**——因此它先于同 Key 后续尚未处理的消息被派发，
  重投仍满足“同 Key 有序、已完成不重复”。
- 重投时若积压仍满，返回 `KindOverflow` 且**死信原样保留**（有界、不静默）。
- 死信是“最终未能投递成功”消息的唯一去处；总线/组 Close 时死信随之释放。

### 3.6 运行态可观测

`g.Stats()` 返回 `GroupStats{OnlineExecutors, Retained, InFlight,
DeadLetters, PerKey}`，便于测试与运维判定“有没有堵、堵在哪个 Key、
有多少在途、多少进了死信”。

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
- **同主题序号连续**：每主题发布锁让“序号分配 + 入队”原子，
  避免多发布者并发导致的暂时缺号被误判为已确认空洞。
- **分担组竞争消费**：组内单派发协程是唯一向执行器通道发送的一方；
  “选活 → 打租约 → 非阻塞入通道”在同一临界区完成，空槽由
  `inflight < queueN` 预留，因此发送必立即成功、无需消费者配合，
  也不会出现同一条被两个执行器同时拿走。
- **同 Key 顺序与掉线转接解耦**：每个 Key 一条队列，只派发队头且队头
  在途则整键等待；Leave 仅释放租约（消息留在队头），新执行者接手的
  仍是同一个队头，换人不换序。等待超时由独立清扫协程负责，处理超时
  由每条消息的看门狗负责，二者与派发协程之间只用组锁 + channel-Cond
  同步，不互相等待、不存在卡死路径。

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
| `TestSubscriptionVisibility`  | 退订后发布立即得到 `DropNoSubscriber` |
| `TestConcurrentOverflowStress`| 慢消费者 + 队列溢出；发布方按 `KindOverflow` 重试，最终不丢不重不死锁 |
| `TestReliableOverflowAndIsolation` | 背压超时返回 `KindOverflow`，且无关主题发布不被阻塞 |
| `TestLowLatencyDropsWithReason`   | 溢出丢弃计入 `DropQueueFull`，账目守恒，发布不阻塞 |
| `TestLowLatencyNoSubscriber`      | 无订阅者给出 `DropNoSubscriber` |
| `TestCodecVersionMismatch` / `TestCodecMissingField` / `TestCodecDecodeGarbage` | 版本/字段/编码异常分别映射为可区分 Kind |
| `TestFrameMarshalRoundTrip`       | 二进制帧编解码往返、坏帧映射 `KindDecode` |
| `TestReceiveDecodeErrors`         | 订阅侧解码拒绝并返回可区分错误 |
| `TestExecCompetingConsumers`      | **多执行器分担**：3 执行器分担 60 条，每条同一时刻唯一属主、恰好完成一次，负载确实均摊 |
| `TestExecFailoverOnLeave`         | **执行器上下线切换**：接手未 Ack 中途掉线，活转接在线者（attempt=2），掉线者同 ID 回来续做；不丢、不重已确认、主题不堵 |
| `TestExecPerKeyOrderAcrossSwitch` | **同 Key 顺序**：3 个机器人键 × 8 条交错发布，中途执行器掉线 + 成员 2→1→3 变化，每个 Key 仍严格按序，不同 Key 并行 |
| `TestExecConcurrentChurnAndOverflow` | **并发上下线 + 积压超限**：执行器持续 flapping，10 条保留消息每人恰好处理一次；满队后可靠 `KindOverflow`、低延迟 `DropQueueFull`，8 条死信持续可查 |
| `TestExecDeadLetterRequeue`       | **死信查看与重投**：溢出死信可查看；积压满时重投被拒且死信保留；排空后重投先于同 Key 新消息、尝试次数重置、不重复 |
| `TestExecTimeoutReasons`          | **超时语义区分**：无人接手 → `DeadWaitTimeout`；接手不 Ack 先转接，达到 `MaxAttempts` → `DeadProcessTimeout(attempts=2)`；与溢出三因可区分 |
| `TestExecGroupTopicIsolation`     | **不连累其他主题**：堆满组快速失败入死信（µs 级），普通主题与另一组发布不受阻塞、不死锁 |

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

### 分担组配置项 `GroupConfig`

| 字段                | 默认值 | 含义 |
| ------------------- | ------ | ---- |
| `MaxBacklog`        | 128    | 组内保留（排队 + 在途未 Ack）消息上限；超限发布立即失败/丢弃并进死信 |
| `ExecutorQueueLen`  | 16     | 每个执行器的在途通道容量（同时在手消息上限） |
| `ProcessTimeoutMs`  | 5000   | 接手后处理超时：先转接，达到 `MaxAttempts` 进死信 |
| `WaitTimeoutMs`     | 30000  | 排队等待任意执行器接手的超时：进死信（与处理超时可区分） |
| `MaxAttempts`       | 3      | 单条消息最多派发次数（含掉线转接） |

### 多执行器分担最小示例

```go
g, _ := bus.CreateExecutorGroup(ctx, "robot/cmd", "line-A", pubsub.GroupConfig{})
ex1, _ := g.Join(ctx, pubsub.ExecutorOption{})
ex2, _ := g.Join(ctx, pubsub.ExecutorOption{}) // 两个执行器一起分担
res, err := bus.PublishToGroup(ctx, "robot/cmd", "line-A",
    pubsub.QoSReliable, cmd, &pubsub.ExecutorGroupKey{Key: "robot-7"})
// 同 robot-7 的命令严格有序；ex1/ex2 之间竞争消费，一条只给一个
// ex 掉线：ex1.Leave(ctx)；回来续做：g.Join(ctx, ExecutorOption{ResumeID: id})
for _, dl := range g.DeadLetters() { /* 查看最终未成功的消息及原因 */ }
_ = g.RequeueDead(ctx, dl.Msg.Seq) // 按原 Key 有序重投
```
