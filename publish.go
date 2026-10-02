package pubsub

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// Publish 发布一条消息。
//
// QoSReliable：对每个在线订阅者做有界背压等待（受 ctx 与配置超时约束），
// 任一订阅者超时未腾出窗口则返回 KindOverflow，已入队的订阅者消息仍保留，
// 不静默丢失；慢订阅者只阻塞本主题本次发布，不影响其他主题/发布者。
//
// QoSLowLatency：绝不阻塞；队列满则丢弃并在结果中给出 DropQueueFull，
// 无在线订阅者给出 DropNoSubscriber。
func (b *Bus) Publish(ctx context.Context, topic string, qos QoS, payload any) (*PublishResult, error) {
	return b.PublishKeyed(ctx, topic, "", qos, payload)
}

// PublishKeyed 同 Publish，但携带顺序键 key（机器人/任务 ID）：
// 执行器组内相同 key 的消息严格按发布顺序投递；普通订阅忽略该键。
func (b *Bus) PublishKeyed(ctx context.Context, topic, key string, qos QoS, payload any) (*PublishResult, error) {
	if topic == "" {
		return nil, newError(KindInvalidArgument, "", "empty topic", nil)
	}
	if !qos.valid() {
		return nil, newError(KindInvalidArgument, topic, fmt.Sprintf("invalid qos %d", qos), nil)
	}
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, newError(KindClosed, topic, "bus already closed", nil)
	}
	schema := b.schemas[topic]
	b.mu.RUnlock()

	var body []byte
	var ver Version
	if schema != nil {
		ver = schema.SchemaVersion()
		raw, err := b.codec.Encode(ctx, schema, payload)
		if err != nil {
			return nil, classifyEncodeErr(topic, err)
		}
		body = raw
	} else {
		// 未注册 schema：直接按 JSON 编码，版本为 0。
		raw, err := b.codec.Encode(ctx, &genericSchema{topic: topic}, payload)
		if err != nil {
			return nil, classifyEncodeErr(topic, err)
		}
		body = raw
	}
	return b.dispatchPublish(ctx, topic, key, ver, qos, body)
}

// PublishRaw 发布已编码的原始字节（绕过 JSON codec；帧/投递层仍生效）。
func (b *Bus) PublishRaw(ctx context.Context, topic string, v Version, qos QoS, body []byte) (*PublishResult, error) {
	return b.PublishRawKeyed(ctx, topic, "", v, qos, body)
}

// PublishRawKeyed 同 PublishRaw，但携带顺序键 key（供执行器组按序分担）。
func (b *Bus) PublishRawKeyed(ctx context.Context, topic, key string, v Version, qos QoS, body []byte) (*PublishResult, error) {
	if topic == "" {
		return nil, newError(KindInvalidArgument, "", "empty topic", nil)
	}
	if !qos.valid() {
		return nil, newError(KindInvalidArgument, topic, fmt.Sprintf("invalid qos %d", qos), nil)
	}
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		return nil, newError(KindClosed, topic, "bus already closed", nil)
	}
	return b.dispatchPublish(ctx, topic, key, v, qos, body)
}

func (b *Bus) dispatchPublish(ctx context.Context, topic, key string, ver Version, qos QoS, body []byte) (*PublishResult, error) {
	// 同一主题的发布在此锁下串行：保证“分配序号 -> 入队”原子，
	// 该主题的未确认序号集合连续，调度器不会把暂时未入队的序号误判
	// 为已确认空洞。等待背压有界（受 ctx/超时约束），不会永久阻塞；
	// 不同主题各持各锁，慢订阅者不阻塞无关主题的发布者。
	topicMu := b.topicLock(topic)
	topicMu.Lock()
	defer topicMu.Unlock()

	seq := b.nextSeq(topic)
	env := &Envelope{
		ID:      newMessageID(),
		Topic:   topic,
		Version: ver,
		Seq:     seq,
		Key:     key,
		Body:    body,
	}

	// 订阅关系与执行器组快照：在总线读锁下取得，增删改对发布立即可见。
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, newError(KindClosed, topic, "bus already closed", nil)
	}
	subs := b.topics[topic]
	snap := make([]*subState, 0, len(subs))
	for _, st := range subs {
		snap = append(snap, st)
	}
	gsnap := make([]*groupState, 0, len(b.groups[topic]))
	for _, g := range b.groups[topic] {
		gsnap = append(gsnap, g)
	}
	b.mu.RUnlock()

	res := &PublishResult{Topic: topic, Seq: seq}

	if len(snap) == 0 && len(gsnap) == 0 {
		if qos == QoSLowLatency {
			// 低延迟：无订阅者不是错误，给出可区分原因。
			res.Dropped = append(res.Dropped, DropInfo{Reason: DropNoSubscriber})
			return res, nil
		}
		// 可靠：没有订阅者时消息无处持久化，返回明确错误而非静默丢失。
		return nil, newError(KindNoSubscriber, topic,
			"reliable publish has no online subscription; message not retained", nil)
	}

	for _, st := range snap {
		st.mu.Lock()
		offline, closed := st.offline, st.closed
		subQos := st.qos
		st.mu.Unlock()
		if closed {
			continue
		}

		// 发布 QoS 与订阅 QoS 取较低者（常见 MQ 语义），并在投递侧一致。
		effQos := qos
		if subQos < effQos {
			effQos = subQos
		}

		switch {
		case offline:
			// 离线订阅者：消息必须保留（续传）。直接入 pending 即可，
			// 不占投递通道；resume 后按序投递。
			st.enqueue(env)
			res.Delivered++
		case effQos == QoSLowLatency:
			// 在线：尽力直接投递，满即丢弃并给出原因；
			// 离线：入 pending 保留以便续传（不静默丢失）。
			if offline {
				st.enqueue(env)
				res.Delivered++
			} else if st.trySendDirect(env) {
				res.Dropped = append(res.Dropped, DropInfo{
					SubscriptionID: st.id, Reason: DropQueueFull,
				})
			} else {
				res.Delivered++
			}
		default:
			// 可靠：有界等待窗口腾出。只阻塞当前发布协程，不影响其他发布。
			if err := b.waitForWindow(ctx, st); err != nil {
				return nil, newError(KindOverflow, topic,
					fmt.Sprintf("subscription %s backpressure timeout at seq=%d", st.id, seq), err)
			}
			st.enqueue(env)
			res.Delivered++
		}
	}

	// 执行器组投递：组是可靠语义载体，消息在组内只由一个成员接手。
	for _, g := range gsnap {
		if qos == QoSLowLatency {
			// 低延迟发布绝不阻塞：组积压未满则入队，满则给出可区分丢弃。
			g.mu.Lock()
			if g.closed {
				g.mu.Unlock()
				continue
			}
			if len(g.pending) >= g.maxPending {
				g.mu.Unlock()
				res.Dropped = append(res.Dropped, DropInfo{
					SubscriptionID: "group:" + g.name, Reason: DropQueueFull,
				})
				continue
			}
			g.enqueueLocked(env)
			g.mu.Unlock()
			res.GroupDelivered++
			continue
		}
		// 可靠发布：组积压超限时做有界等待（受 ctx/配置超时约束），
		// 只阻塞本主题本次发布，不影响其他主题的发布者。
		if err := b.waitGroupWindow(ctx, g); err != nil {
			return nil, newError(KindOverflow, topic,
				fmt.Sprintf("group %s backlog timeout at seq=%d", g.name, seq), err)
		}
		g.mu.Lock()
		if !g.closed {
			g.enqueueLocked(env)
			res.GroupDelivered++
		}
		g.mu.Unlock()
	}
	return res, nil
}

// waitGroupWindow 等待执行器组积压降到上限之下（Ack/判死推进）。
func (b *Bus) waitGroupWindow(ctx context.Context, g *groupState) error {
	timeout := time.Duration(b.cfg.ReliableEnqueueTimeoutMs) * time.Millisecond
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		g.mu.Lock()
		closed := g.closed
		full := len(g.pending) >= g.maxPending
		g.mu.Unlock()
		if closed {
			return fmt.Errorf("group %s closed", g.name)
		}
		if !full {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("group enqueue timeout after %s", timeout)
		case <-tick.C:
		}
	}
}

// waitForWindow 等待订阅者未完成消息数小于容量（Ack/消费推进）。
func (b *Bus) waitForWindow(ctx context.Context, st *subState) error {
	timeout := time.Duration(b.cfg.ReliableEnqueueTimeoutMs) * time.Millisecond
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if st.inflightCount() < st.queueN {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("enqueue timeout after %s", timeout)
		case <-tick.C:
		}
	}
}

func (b *Bus) nextSeq(topic string) uint64 {
	b.seqMu.Lock()
	defer b.seqMu.Unlock()
	b.seqs[topic]++
	return b.seqs[topic]
}

func newMessageID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func classifyEncodeErr(topic string, err error) error {
	if pe, ok := err.(*Error); ok {
		pe.Topic = topic
		return pe
	}
	return newError(KindDecode, topic, "encode failed", err)
}

// genericSchema 未注册 schema 时的兜底（无必填字段、版本 0）。
type genericSchema struct {
	topic string
}

func (g *genericSchema) TopicName() string        { return g.topic }
func (g *genericSchema) SchemaVersion() Version   { return 0 }
func (g *genericSchema) RequiredFields() []string { return nil }
