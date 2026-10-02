package pubsub

import (
	"context"
	"fmt"
	"time"
)

// ExecutorGroupKey 是发布选项中的顺序键：同一 (topic, group) 下，
// 同一个 Key 的消息始终严格按发布先后顺序处理，即使中途换执行器。
// 键通常取“机器人 ID”或“任务 ID”；为空（或 opt 为 nil）时归入
// 默认键，默认键内同样严格有序。
type ExecutorGroupKey struct {
	Key string
}

// defaultGroupKey 未声明顺序键时归入的键。
const defaultGroupKey = ""

// CreateExecutorGroup 在指定主题上创建/复用一个多执行器分担组。
// 同一对 (topic, name) 重复创建返回既有组（幂等）。
func (b *Bus) CreateExecutorGroup(ctx context.Context, topic, name string, cfg GroupConfig) (*ExecutorGroup, error) {
	if topic == "" || name == "" {
		return nil, newError(KindInvalidArgument, "", "topic and group name are required", nil)
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, newError(KindClosed, topic, "bus already closed", nil)
	}
	if byTopic, ok := b.groups[topic]; ok {
		if existing, ok := byTopic[name]; ok {
			b.mu.Unlock()
			return &ExecutorGroup{topic: topic, name: name, bus: b, st: existing}, nil
		}
	}
	schema := b.schemas[topic]
	st := newGroupState(topic, name, cfg, schema, b.codec)
	if b.groups[topic] == nil {
		b.groups[topic] = map[string]*groupState{}
	}
	b.groups[topic][name] = st
	b.mu.Unlock()

	go b.groupDispatchLoop(st)
	go b.groupJanitor(st)
	return &ExecutorGroup{topic: topic, name: name, bus: b, st: st}, nil
}

// PublishToGroup 向指定主题的分担组发布一条消息。
//
// 消息按组配置保留：组存在时即使没有在线执行器也保留待处理，不抛
// KindNoSubscriber；积压超过 MaxBacklog 时：
//   - 可靠发布：返回 KindOverflow；
//   - 低延迟发布：不阻塞，PublishResult.Dropped 给出 DropQueueFull；
//
// 两种情况下消息都进入该组死信队列（DeadBacklogOverflow），
// 可查看、可重新投递。发布本身绝不为“等执行器处理”而阻塞。
//
// opt 可为 *ExecutorGroupKey（或为 nil），用于声明同 Key 顺序。
func (b *Bus) PublishToGroup(ctx context.Context, topic, groupName string, qos QoS, payload any, opt any) (*PublishResult, error) {
	if topic == "" {
		return nil, newError(KindInvalidArgument, "", "empty topic", nil)
	}
	if !qos.valid() {
		return nil, newError(KindInvalidArgument, topic, fmt.Sprintf("invalid qos %d", qos), nil)
	}
	key := defaultGroupKey
	if gk, ok := opt.(*ExecutorGroupKey); ok && gk != nil {
		key = gk.Key
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
		raw, err := b.codec.Encode(ctx, &genericSchema{topic: topic}, payload)
		if err != nil {
			return nil, classifyEncodeErr(topic, err)
		}
		body = raw
	}
	return b.enqueueGroup(topic, groupName, ver, qos, body, key)
}

// PublishRawToGroup 向分担组发布已编码字节（绕过 JSON codec；帧/组层仍生效）。
func (b *Bus) PublishRawToGroup(ctx context.Context, topic, groupName string, v Version, qos QoS, body []byte, key string) (*PublishResult, error) {
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
	return b.enqueueGroup(topic, groupName, v, qos, body, key)
}

// enqueueGroup 把消息编入分担组：
//
//	序号分配与入队在每主题发布锁下原子完成（与普通订阅共用同一主题
//	序号空间与同一把锁），同主题普通发布者不会被组处理速度阻塞；
//	组积压满时立即给出结论并把消息送入死信队列，绝不阻塞等待执行器。
func (b *Bus) enqueueGroup(topic, groupName string, ver Version, qos QoS, body []byte, key string) (*PublishResult, error) {
	topicMu := b.topicLock(topic)
	topicMu.Lock()
	defer topicMu.Unlock()

	b.mu.RLock()
	var st *groupState
	if byTopic := b.groups[topic]; byTopic != nil {
		st = byTopic[groupName]
	}
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		return nil, newError(KindClosed, topic, "bus already closed", nil)
	}
	if st == nil {
		// 组不存在：消息无处保留，按可区分结论返回（底层异常不透出）。
		if qos == QoSLowLatency {
			return &PublishResult{Topic: topic, Dropped: []DropInfo{{Reason: DropNoSubscriber}}}, nil
		}
		return nil, newError(KindNoSubscriber, topic,
			"executor group not found: "+groupName+"; message not retained", nil)
	}

	seq := b.nextSeq(topic)
	env := &Envelope{ID: newMessageID(), Topic: topic, Version: ver, Seq: seq, Body: body}
	res := &PublishResult{Topic: topic, Seq: seq}

	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return nil, newError(KindClosed, topic, "executor group already closed", nil)
	}
	if st.retainedLocked() >= st.cfg.MaxBacklog {
		// 积压超限：立即失败/丢弃并落入死信队列，发布者不被拖住。
		st.addDeadLocked(&queuedGroupMsg{env: env, key: key, enqueuedAt: time.Now()},
			DeadBacklogOverflow, 0)
		st.mu.Unlock()
		if qos == QoSLowLatency {
			res.Dropped = append(res.Dropped, DropInfo{SubscriptionID: groupName, Reason: DropQueueFull})
			return res, nil
		}
		return nil, newError(KindOverflow, topic,
			fmt.Sprintf("executor group %s backlog overflow at seq=%d (limit=%d); moved to dead-letter queue",
				groupName, seq, st.cfg.MaxBacklog), nil)
	}
	qm := &queuedGroupMsg{env: env, key: key, enqueuedAt: time.Now()}
	st.pushKeyLocked(qm)
	res.Delivered = 1
	st.signalLocked()
	st.mu.Unlock()
	return res, nil
}
