package pubsub

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// queuedMsg 是订阅队列中的一个排队项。
// qos 在入队时盖戳：之后即使订阅 QoS 被修改，本条消息仍按入队时的等级裁决。
type queuedMsg struct {
	env     *Envelope
	qos     QoS
	attempt int
	token   uint64 // 每次投递尝试的唯一令牌；用于裁决有效性，防止旧投递误判新尝试
}

// Subscription 表示一个客户端对某主题的订阅关系及其会话状态。
// 一个 (clientID, topic) 至多对应一个 Subscription：短暂离线后用相同
// clientID 重新 Subscribe 会恢复同一会话，未确认消息继续重投。
type Subscription struct {
	id       string
	clientID string
	topic    string
	log      Logger
	busDone  <-chan struct{}
	capacity int

	mu         sync.Mutex
	qos        QoS
	epoch      uint64
	connected  bool
	closed     bool
	handler    Handler
	rootCtx    context.Context // 随总线关闭/会话删除而取消，低延迟回调使用
	rootCancel context.CancelFunc
	connCtx    context.Context // 每次连接一个；detach 时取消，高可靠在途尝试使用
	cancel     context.CancelFunc

	queue    *list.List    // 队首可能是“在途”项（已派发未裁决）
	notFull  *sync.Cond    // 唤醒等待容量的高可靠发布者
	changed  *sync.Cond    // 唤醒派发循环：入队/裁决/槽位释放/连接变化（Signal 不丢失）
	stop     chan struct{} // 停止总线关闭监听 goroutine
	supersed chan struct{} // 当前在途尝试的失效信号；断连/关闭时关闭以作废旧投递
	limit    *rateLimiter

	// inflight 是低延迟等级当前尚未处理完的回调数；maxInflight 是其上界。
	// 把在途计入容量后，慢消费者会真实地占满容量从而触发丢弃，而发布者绝不阻塞。
	inflight    int
	maxInflight int

	// attemptGen 为每次取出队首的投递尝试生成唯一令牌。
	// 注意：不能用 list.Element 指针判定裁决归属——list 在删除/重排后可能复用内存，
	// 快速重连时旧 handler 的迟到 Ack 会因此误删重连后的新尝试，造成重复投递。
	attemptGen uint64

	// preDispatch 在每次取出队首前被调用（仅用于测试中稳定制造队列积压）。
	preDispatch func()

	// 判定计数（在 mu 下更新），供测试断言与日志说明。
	statEnqueued  int64
	statDelivered int64
	statRedeliver int64
	statAcked     int64
	statNacked    int64
	statDropped   map[DropReason]int64
}

// SubStats 是订阅会话判定计数的快照。
type SubStats struct {
	Epoch              uint64
	Connected          bool
	Enqueued           int64
	Delivered          int64
	Redelivered        int64
	Acked              int64
	Nacked             int64
	DroppedByReason    map[DropReason]int64
	CurrentQueueLength int
}

func newSubscription(id, clientID, topic string, qos QoS, capacity, maxInflight int,
	h Handler, log Logger, busDone <-chan struct{}, limit *rateLimiter) *Subscription {
	s := &Subscription{
		id: id, clientID: clientID, topic: topic, log: log, busDone: busDone,
		capacity: capacity, maxInflight: maxInflight, qos: qos, handler: h,
		queue: list.New(),
		limit: limit, statDropped: map[DropReason]int64{},
	}
	s.rootCtx, s.rootCancel = context.WithCancel(context.Background())
	s.notFull = sync.NewCond(&s.mu)
	s.changed = sync.NewCond(&s.mu)
	s.stop = make(chan struct{})
	// 唯一的总线关闭监听者：总线 done 后广播 changed，唤醒可能在空队列上等待的派发循环。
	go func() {
		select {
		case <-s.busDone:
			s.mu.Lock()
			s.changed.Broadcast()
			s.mu.Unlock()
		case <-s.stop:
		}
	}()
	return s
}

// ID 返回订阅 ID（会话内稳定，重连不变）。
func (s *Subscription) ID() string { return s.id }

// ClientID 返回订阅者客户端 ID（重连续传的依据）。
func (s *Subscription) ClientID() string { return s.clientID }

// Topic 返回主题。
func (s *Subscription) Topic() string { return s.topic }

// QoS 返回当前服务质量等级。
func (s *Subscription) QoS() QoS {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.qos
}

// Epoch 在订阅关系每次变更（重连/改 QoS）时递增，作为一致性版本号。
func (s *Subscription) Epoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch
}

// Stats 返回判定计数快照。
func (s *Subscription) Stats() SubStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	drops := make(map[DropReason]int64, len(s.statDropped))
	for k, v := range s.statDropped {
		drops[k] = v
	}
	return SubStats{
		Epoch: s.epoch, Connected: s.connected,
		Enqueued: s.statEnqueued, Delivered: s.statDelivered,
		Redelivered: s.statRedeliver, Acked: s.statAcked, Nacked: s.statNacked,
		DroppedByReason: drops, CurrentQueueLength: s.queue.Len(),
	}
}

// signal 唤醒派发循环。使用 sync.Cond.Signal：通知不会像容量 1 通道那样被合并丢失，
// 配合“持锁等待、持锁重检谓词”可消除 lost wakeup。
func (s *Subscription) signal() {
	s.changed.Signal()
}

// waitWhile 的调用方必须持有 s.mu；它原子地释放锁并等待，被唤醒后重新加锁返回。
// 总线关闭由构造时启动的唯一监听 goroutine 通过 Broadcast 唤醒，不会永久沉睡。
func (s *Subscription) waitWhile() {
	s.changed.Wait()
}

// connect 建立/恢复连接，必须在 bus 映射锁之外调用（只取 sub.mu）。
func (s *Subscription) connect(h Handler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return wrapError(KindClosed, "subscribe", s.topic, "session deleted", nil)
	}
	if s.connected {
		return wrapError(KindConflict, "subscribe", s.topic,
			"client already connected: "+s.clientID, ErrAlreadyExists)
	}
	s.handler = h
	s.connected = true
	s.epoch++
	s.connCtx, s.cancel = context.WithCancel(s.rootCtx)
	s.signal()
	s.log.Printf("[session] sub=%s client=%s topic=%s connected/resumed epoch=%d queue=%d",
		s.id, s.clientID, s.topic, s.epoch, s.queue.Len())
	return nil
}

// detach 短暂离线：保留会话与队列，作废旧的在途投递（高可靠消息稍后按序重投）。
func (s *Subscription) detach() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.connected || s.closed {
		return
	}
	s.connected = false
	s.epoch++
	if s.cancel != nil {
		s.cancel()
	}
	s.invalidateInflightLocked()
	s.log.Printf("[session] sub=%s client=%s topic=%s detached epoch=%d pending=%d",
		s.id, s.clientID, s.topic, s.epoch, s.queue.Len())
}

// terminate 删除会话：作废旧投递并让派发循环退出。
func (s *Subscription) terminate() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.connected = false
	s.epoch++
	if s.cancel != nil {
		s.cancel() // 取消当前连接的在途尝试
	}
	s.rootCancel() // 唤醒可能仍阻塞在低延迟回调里的用户代码
	s.invalidateInflightLocked()
	n := s.queue.Len()
	s.queue.Init()
	s.inflight = 0
	s.notFull.Broadcast()
	s.changed.Broadcast() // 唤醒在空队列/槽位上等待的派发循环，使其看到 closed 并退出
	s.mu.Unlock()
	close(s.stop) // 停止总线关闭监听 goroutine（若总线未关闭）
	s.log.Printf("[session] sub=%s client=%s topic=%s terminated discarded=%d",
		s.id, s.clientID, s.topic, n)
}

// invalidateInflightLocked 关闭当前 supersede 通道，使旧 handler 的等待结论失效。
func (s *Subscription) invalidateInflightLocked() {
	if s.supersed != nil {
		close(s.supersed)
		s.supersed = nil
	}
}

// setQoS 修改等级：对后续入队消息立即可见；已在队消息保留入队时盖戳的等级。
func (s *Subscription) setQoS(q QoS) {
	s.mu.Lock()
	s.qos = q
	s.epoch++
	s.mu.Unlock()
	s.log.Printf("[session] sub=%s topic=%s qos->%s epoch=%d", s.id, s.topic, q, s.Epoch())
}

// enqueueReliable 高可靠入队：队列满时等待容量或 ctx 取消；不持有任何总线级锁，
// 因此阻塞不会影响无关主题/订阅的发布者。
func (s *Subscription) enqueueReliable(ctx context.Context, env *Envelope) error {
	s.mu.Lock()
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.notFull.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	for s.queue.Len() >= s.capacity && !s.closed && ctx.Err() == nil {
		s.log.Printf("[backpressure] sub=%s topic=%s queue=%d full, publisher waits",
			s.id, s.topic, s.queue.Len())
		s.notFull.Wait()
	}
	if s.closed {
		s.mu.Unlock()
		return wrapError(KindClosed, "publish", s.topic, "session deleted", ErrBusClosed)
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return wrapError(KindBackpressure, "publish", s.topic,
			"reliable queue full and context canceled while waiting", err)
	}
	s.queue.PushBack(&queuedMsg{env: env, qos: QoSReliable})
	s.statEnqueued++
	s.signal()
	s.mu.Unlock()
	return nil
}

// enqueueLowLatency 低延迟入队：绝不阻塞，返回丢弃原因（DropNone 表示入队成功）。
func (s *Subscription) enqueueLowLatency(env *Envelope, policy DropPolicy) DropReason {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.closed:
		s.statDropped[DropSubscriptionClosed]++
		return DropSubscriptionClosed
	case !s.connected:
		s.statDropped[DropSubscriberOffline]++
		return DropSubscriberOffline
	case s.limit != nil && !s.limit.allow(time.Now()):
		s.statDropped[DropRateLimited]++
		return DropRateLimited
	}
	// queue.Len() 已包含“在途”（高可靠在途项留在队首；低延迟在途由 inflight 计数），
	// 因此低延迟占用 = 队列长度 + 在途数。
	if s.queue.Len()+s.inflight >= s.capacity {
		if policy == DropOldest {
			if evicted := s.evictOldestLowLatencyLocked(); evicted {
				s.statDropped[DropQueueFull]++
				s.log.Printf("[drop] sub=%s topic=%s seq=%d reason=%s (drop-oldest, new seq=%d)",
					s.id, s.topic, env.Seq-1, DropQueueFull, env.Seq)
			} else {
				s.statDropped[DropQueueFull]++
				s.log.Printf("[drop] sub=%s topic=%s seq=%d reason=%s (no evictable best-effort msg)",
					s.id, s.topic, env.Seq, DropQueueFull)
				return DropQueueFull
			}
		} else {
			s.statDropped[DropQueueFull]++
			s.log.Printf("[drop] sub=%s topic=%s seq=%d reason=%s (drop-newest)",
				s.id, s.topic, env.Seq, DropQueueFull)
			return DropQueueFull
		}
	}
	s.queue.PushBack(&queuedMsg{env: env, qos: QoSLowLatency})
	s.statEnqueued++
	s.signal()
	return DropNone
}

// evictOldestLowLatencyLocked 淘汰队首之后最早的低延迟消息；
// 队首若是在途的高可靠消息，绝不淘汰。
func (s *Subscription) evictOldestLowLatencyLocked() bool {
	first := true
	for e := s.queue.Front(); e != nil; e = e.Next() {
		if first { // 跳过可能在途的队首
			first = false
			continue
		}
		if m := e.Value.(*queuedMsg); m.qos == QoSLowLatency {
			s.queue.Remove(e)
			return true
		}
	}
	return false
}

// run 是每个订阅一个的派发循环，保证同主题顺序：上一条高可靠消息未裁决，不派下一条。
func (s *Subscription) run() {
dispatch:
	for {
		s.mu.Lock()
		for {
			if s.closed {
				s.mu.Unlock()
				return
			}
			if !s.connected || s.queue.Front() == nil {
				s.waitWhile() // 内部先释放锁、等待后再加锁返回
				continue
			}
			head := s.queue.Front().Value.(*queuedMsg)
			// 低延迟在途槽位占满时等待某回调结束；不丢弃队首（队列里的等待空槽）。
			if head.qos == QoSLowLatency && s.inflight >= s.maxInflight {
				s.waitWhile()
				continue
			}
			break
		}
		if s.preDispatch != nil {
			hook := s.preDispatch
			s.mu.Unlock()
			hook()
			continue dispatch // 回到顶部重新加锁取队首，避免持锁 continue 自锁
		}
		front := s.queue.Front()
		msg := front.Value.(*queuedMsg)
		msg.attempt++
		s.attemptGen++
		msg.token = s.attemptGen
		token := msg.token
		d := newDelivery(msg.env, s, msg.attempt)
		qos := msg.qos
		// 高可靠：每次尝试派生可取消 ctx，detach/裁决时取消；
		// 低延迟：直接使用订阅根 ctx（仅 Close/删除会话时取消），
		// 不能在派发后立即取消，否则 handler 的 ctx 一开始就失效、无法模拟慢消费者。
		var (
			ctx    context.Context
			cancel context.CancelFunc
		)
		if qos == QoSReliable {
			ctx, cancel = context.WithCancel(s.connCtx)
		} else {
			ctx = s.rootCtx
			cancel = func() {} // 低延迟不派生 ctx，占位以统一 defer 调用
		}
		supCh := make(chan struct{})
		s.supersed = supCh
		h := s.handler
		s.statDelivered++
		if msg.attempt > 1 {
			s.statRedeliver++
		}
		if qos == QoSLowLatency {
			// 关键：必须在持锁阶段（解锁前）移除队首并占用在途槽位，
			// 否则解锁后、弹出前若漏斗把下一条入队，队首令牌变化会导致漏弹与重复派发。
			s.queue.Remove(front)
			s.notFull.Broadcast()
			s.inflight++
		}
		s.mu.Unlock()

		s.log.Printf("[deliver] sub=%s client=%s topic=%s seq=%d id=%s qos=%s attempt=%d redelivered=%v",
			s.id, s.clientID, s.topic, msg.env.Seq, msg.env.ID, qos, msg.attempt, msg.attempt > 1)

		if qos == QoSLowLatency {
			// fire-and-forget：回调异步执行；其返回即归还在途槽位，保证 Close 也能回收。
			go func() {
				defer cancel()
				defer s.releaseLowLatencySlot()
				s.safeHandle(h, ctx, d)
			}()
			continue
		}

		// 高可靠：在途项留在队首，单 goroutine 执行 handler 并等待裁决，保证严格顺序。
		go s.safeHandle(h, ctx, d)

		select {
		case v := <-d.done:
			cancel()
			// supersede 与 Ack 可能同时就绪：必须先确认本次投递未被作废，
			// 否则旧 handler 的迟到裁决会错误地移除重连后正在重投的消息。
			select {
			case <-supCh:
				s.log.Printf("[verdict-ignored] sub=%s topic=%s seq=%d stale verdict after supersede",
					s.id, s.topic, msg.env.Seq)
				continue
			default:
			}
			if v == verdictAck {
				s.ackInflight(token, msg)
			} else {
				s.nackInflight(token, msg)
			}
		case <-supCh:
			cancel()
			s.log.Printf("[redeliver-pending] sub=%s topic=%s seq=%d attempt invalidated, will redeliver on resume",
				s.id, s.topic, msg.env.Seq)
		case <-s.busDone:
			cancel()
			return
		}
	}
}

func (s *Subscription) safeHandle(h Handler, ctx context.Context, d *Delivery) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Printf("[handler-panic] sub=%s topic=%s seq=%d recover=%v -> nack",
				s.id, s.topic, d.Envelope.Seq, r)
			d.Nack()
		}
	}()
	h(ctx, d)
}

// removeInflightLocked 仅当队首仍是本次在途尝试（令牌匹配）时将其移除。
// 令牌不匹配说明该在途项已因断连/会话变更失效并已被新尝试取代，本次裁决作废。
func (s *Subscription) removeInflightLocked(token uint64) bool {
	if e := s.queue.Front(); e != nil {
		if m := e.Value.(*queuedMsg); m.token == token {
			s.queue.Remove(e)
			s.notFull.Broadcast()
			return true
		}
	}
	return false
}

// releaseLowLatencySlot 低延迟回调结束后归还在途槽位。
func (s *Subscription) releaseLowLatencySlot() {
	s.mu.Lock()
	if s.inflight > 0 {
		s.inflight--
	}
	s.notFull.Broadcast()
	s.signal()
	s.mu.Unlock()
}

// ackInflight 处理高可靠确认：只有令牌仍匹配（未被 supersede）才真正确认出队；
// 已确认的消息令牌随即作废，任何旧 handler 的迟到 Ack 都无法再影响后续消息。
func (s *Subscription) ackInflight(token uint64, msg *queuedMsg) {
	s.mu.Lock()
	matched := s.removeInflightLocked(token)
	if matched {
		s.statAcked++
	}
	s.signal()
	s.mu.Unlock()
	if matched {
		s.log.Printf("[verdict] sub=%s topic=%s seq=%d acked (no further delivery)",
			s.id, s.topic, msg.env.Seq)
	} else {
		s.log.Printf("[verdict-ignored] sub=%s topic=%s seq=%d late ack token=%d",
			s.id, s.topic, msg.env.Seq, token)
	}
}

// nackInflight 处理高可靠拒绝：消息保留在队首，下一轮以新令牌重投。
func (s *Subscription) nackInflight(token uint64, msg *queuedMsg) {
	s.mu.Lock()
	if e := s.queue.Front(); e == nil || e.Value.(*queuedMsg).token != token {
		s.mu.Unlock()
		s.log.Printf("[verdict-ignored] sub=%s topic=%s seq=%d late nack token=%d",
			s.id, s.topic, msg.env.Seq, token)
		return
	}
	s.statNacked++
	s.signal()
	s.mu.Unlock()
	s.log.Printf("[verdict] sub=%s topic=%s seq=%d nacked, requeue-at-head, next attempt=%d",
		s.id, s.topic, msg.env.Seq, msg.attempt+1)
	time.Sleep(nackBackoff)
}

// DropPolicy 决定低延迟队列满时的策略。
type DropPolicy int

const (
	// DropNewest 丢弃新来的消息（默认）。
	DropNewest DropPolicy = iota
	// DropOldest 淘汰队列中最早的可丢弃低延迟消息。
	DropOldest
)

// rateLimiter 是极简令牌桶，仅用于低延迟等级的限速保护。
type rateLimiter struct {
	rate   float64 // 每秒令牌
	tokens float64
	last   time.Time
}

func newRateLimiter(perSecond float64) *rateLimiter {
	if perSecond <= 0 {
		return nil
	}
	return &rateLimiter{rate: perSecond, tokens: perSecond, last: time.Now()}
}

func (l *rateLimiter) allow(now time.Time) bool {
	if l == nil {
		return true
	}
	if !l.last.IsZero() {
		l.tokens += now.Sub(l.last).Seconds() * l.rate
		if l.tokens > l.rate {
			l.tokens = l.rate
		}
	}
	l.last = now
	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}
