package pubsub

import (
	"context"
)

// SubscribeOption 订阅参数。
type SubscribeOption struct {
	// QueueLen 订阅者待确认队列容量（0 表示用总线默认值）。
	QueueLen int
	// ResumeID 非空时按既有订阅者 ID 续传；为空表示新订阅。
	ResumeID string
}

// Subscription 订阅句柄。一个 Subscription 的生命周期：
//
//	新建(active) --pause--> offline --resume--> active ... --Close--> removed
//
// offline 期间未 Ack 的消息与后续新消息都由总线保留，resume 后继续投递；
// Ack 过的消息不会再次投递。
type Subscription struct {
	id    string
	topic string
	qos   QoS
	st    *subState
	bus   *Bus
}

// ID 返回订阅者 ID（重连续传时复用）。
func (s *Subscription) ID() string { return s.id }

// Topic 返回订阅主题。
func (s *Subscription) Topic() string { return s.topic }

// QoS 返回订阅协商后的 QoS。
func (s *Subscription) QoS() QoS { return s.qos }

// Messages 返回当前接收通道；resume 续传后返回重建的新通道，
// 调用方应重新读取本句柄（通过同一 Subscription 对象）。
func (s *Subscription) Messages() <-chan *Delivery {
	if s.st == nil {
		return nil
	}
	return s.st.guard.get()
}

// Ack 确认一条消息。可靠等级下：
//   - 已确认的消息被标记为已完成，永不再投递；
//   - 允许按乱序 Ack，但小于等于已确认水位的 Seq 视为幂等重复确认。
func (s *Subscription) Ack(seq uint64) error {
	return s.bus.ack(s, seq)
}

// Close 退订并销毁订阅状态（与离线重连不同：之后不可再续传）。
func (s *Subscription) Close() error {
	return s.bus.removeSubscription(s)
}

// Unsubscribe 是 Close 的语义别名。
func (s *Subscription) Unsubscribe() error { return s.Close() }

// Pause 模拟订阅者短暂离线：不再推送（消费侧停止读取即可），
// 但总线保留订阅与未确认/后续消息。
func (s *Subscription) Pause(_ context.Context) error {
	return s.bus.setOffline(s, true)
}

// Resume 重新接入：继续接收积压与新消息。
func (s *Subscription) Resume(_ context.Context) error {
	return s.bus.setOffline(s, false)
}
