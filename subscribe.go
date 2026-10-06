package pubsub

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Subscribe 订阅主题。
//
// opt.ResumeID 为空：创建新订阅者，只接收订阅之后发布的消息。
// opt.ResumeID 非空：按既有订阅者续传；若该订阅已 Close 则返回错误。
// 续传与 Resume 等价：投递通道重建（调用方应重新读取 Messages），
// 旧通道上未消费的消息不再推送，未 Ack 与订阅期间错过的消息
// 会按主题序号顺序重新/继续投递，已 Ack 的消息不会重复。
//
// 订阅关系的增删改在总线互斥下完成，对所有参与者立即可见。
func (b *Bus) Subscribe(_ context.Context, topic string, qos QoS, opt SubscribeOption) (*Subscription, error) {
	if topic == "" {
		return nil, newError(KindInvalidArgument, "", "empty topic", nil)
	}
	if !qos.valid() {
		return nil, newError(KindInvalidArgument, topic, fmt.Sprintf("invalid qos %d", qos), nil)
	}
	queueN := opt.QueueLen
	if queueN <= 0 {
		queueN = b.cfg.DefaultQueueLen
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, newError(KindClosed, topic, "bus already closed", nil)
	}

	var st *subState
	var oldCh chan *Delivery
	var oldWg *sync.WaitGroup
	if opt.ResumeID != "" {
		existing := b.byID[opt.ResumeID]
		if existing == nil || existing.topic != topic {
			b.mu.Unlock()
			return nil, newError(KindInvalidArgument, topic,
				"cannot resume: subscription id not found for topic: "+opt.ResumeID, nil)
		}
		st = existing
		st.mu.Lock()
		if st.closed {
			st.mu.Unlock()
			b.mu.Unlock()
			return nil, newError(KindInvalidArgument, topic,
				"cannot resume a closed subscription: "+opt.ResumeID, nil)
		}
		// 订阅参数变更（重连时允许改 QoS / 队列容量）立即生效。
		st.qos = qos
		st.queueN = queueN
		st.mu.Unlock()
		// 续传必须走与 Resume 相同的重置：重建通道、未 Ack 消息
		// 重置为待投递、调度水位回到最小未确认序号、代际号自增。
		// 否则旧通道中滞留的 inCh 消息与陈旧水位会把调度卡死
		// （消费者等不到消息），或与旧状态叠加造成乱序。
		oldCh, oldWg = st.resetForResume()
	} else {
		st = b.newSubStateLocked(topic, qos, queueN)
		subs := b.topics[topic]
		if subs == nil {
			subs = map[string]*subState{}
			b.topics[topic] = subs
		}
		subs[st.id] = st
		b.byID[st.id] = st
	}

	// 每个订阅关系同一时刻只允许一个调度协程：Pause 不会让旧协程
	// 退出（它只是等待），续传若再起一个，多个发送协程会在锁外
	// 并发向同一通道发送，先后无法保证，导致乱序/跳号。
	st.mu.Lock()
	startLoop := !st.loopRunning
	if startLoop {
		st.loopRunning = true
	}
	st.mu.Unlock()
	b.mu.Unlock()

	if startLoop {
		go b.dispatchLoop(st)
	}
	if oldCh != nil {
		// resetForResume 已关闭旧 abortCh 解除在途阻塞发送；
		// 只等待旧代际发送归零（新代际在另一个 WaitGroup 上计数），
		// 再关闭旧通道（消除 close/send 竞态）。
		oldWg.Wait()
		safeCloseCh(oldCh)
	}

	return &Subscription{id: st.id, topic: topic, qos: qos, st: st, bus: b}, nil
}

func (b *Bus) newSubStateLocked(topic string, qos QoS, queueN int) *subState {
	n := atomic.AddUint64(&b.idCounter, 1)
	ch := make(chan *Delivery, queueN)
	st := &subState{
		id:        fmt.Sprintf("sub-%d", n),
		topic:     topic,
		qos:       qos,
		queueN:    queueN,
		ch:        ch,
		guard:     &chGuard{ch: ch},
		waitCh:    make(chan struct{}),
		abortCh:   make(chan struct{}),
		pending:   map[uint64]*queuedMsg{},
		redelivMs: b.cfg.RedeliveryTimeoutMs,
		sendWg:    &sync.WaitGroup{},
	}
	return st
}

func (b *Bus) removeSubscription(s *Subscription) error {
	b.mu.Lock()
	st := b.byID[s.id]
	b.mu.Unlock()
	if st == nil {
		return nil
	}
	st.mu.Lock()
	st.closed = true
	st.offline = false
	st.pending = map[uint64]*queuedMsg{}
	ch := st.guard.get()
	abort := st.abortCh
	st.abortCh = nil
	wg := st.sendWg
	st.signalLocked()
	st.mu.Unlock()
	// 先让在途阻塞发送退出并归零，再关闭通道（消除 close/send 竞态）。
	// closed 已在锁内标记，不会再有新的发送登记到该代际。
	if abort != nil {
		close(abort)
	}
	wg.Wait()
	safeCloseCh(ch)

	b.mu.Lock()
	if subs := b.topics[st.topic]; subs != nil {
		delete(subs, st.id)
		if len(subs) == 0 {
			delete(b.topics, st.topic)
		}
	}
	delete(b.byID, st.id)
	b.mu.Unlock()
	return nil
}

func (b *Bus) setOffline(s *Subscription, offline bool) error {
	b.mu.RLock()
	st := b.byID[s.id]
	b.mu.RUnlock()
	if st == nil {
		return newError(KindInvalidArgument, s.topic, "subscription not found", nil)
	}
	if offline {
		st.mu.Lock()
		if st.closed {
			st.mu.Unlock()
			return newError(KindInvalidArgument, s.topic, "subscription closed", nil)
		}
		st.offline = true
		st.signalLocked()
		st.mu.Unlock()
		return nil
	}
	// resume：重建投递通道，未 Ack 消息全部重置为待投递；
	// offline=false 在 resetForResume 内与状态重排同锁完成，
	// 不会出现“已上线但水位/通道还是旧的”的中间窗口。
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return newError(KindInvalidArgument, s.topic, "subscription closed", nil)
	}
	st.mu.Unlock()
	old, oldWg := st.resetForResume()
	// resetForResume 已关闭旧 abortCh 解除在途阻塞发送；
	// 只等旧代际发送归零，再关闭旧通道（消除 close/send 竞态）。
	oldWg.Wait()
	safeCloseCh(old)
	return nil
}

func (b *Bus) ack(s *Subscription, seq uint64) error {
	b.mu.RLock()
	st := b.byID[s.id]
	b.mu.RUnlock()
	if st == nil {
		return newError(KindInvalidArgument, s.topic, "subscription not found", nil)
	}
	return st.ack(seq)
}

func safeCloseCh(ch chan *Delivery) (closed bool) {
	defer func() {
		if r := recover(); r != nil {
			closed = false
		}
	}()
	close(ch)
	return true
}
