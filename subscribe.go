package pubsub

import (
	"context"
	"fmt"
	"sync/atomic"
)

// Subscribe 订阅主题。
//
// opt.ResumeID 为空：创建新订阅者，只接收订阅之后发布的消息。
// opt.ResumeID 非空：按既有订阅者续传；若该订阅已 Close 则返回错误。
// 续传时旧通道上未消费的消息不再推送，未 Ack 与订阅期间错过的消息
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
		st.mu.Unlock()
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

	// 订阅参数变更（重连时允许改 QoS / 队列容量）立即对所有参与者生效：
	// 发布侧每次 publish 都重新快照订阅表，此处只更新状态。
	st.mu.Lock()
	st.qos = qos
	st.queueN = queueN
	st.offline = false
	st.mu.Unlock()
	b.mu.Unlock()

	go b.dispatchLoop(st)

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
	st.signalLocked()
	st.mu.Unlock()
	// 先让在途阻塞发送退出并归零，再关闭通道（消除 close/send 竞态）。
	if abort != nil {
		close(abort)
	}
	st.sendWg.Wait()
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
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return newError(KindInvalidArgument, s.topic, "subscription closed", nil)
	}
	st.offline = offline
	st.signalLocked()
	st.mu.Unlock()
	if !offline {
		// resume：重建投递通道，未 Ack 消息全部重置为待投递；
		// resetForResume 已关闭旧 abortCh 解除在途阻塞发送，
		// 等其归零后再关闭旧通道（消除 close/send 竞态）。
		old := st.resetForResume()
		st.sendWg.Wait()
		safeCloseCh(old)
	}
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
