package pubsub

import (
	"context"
	"sync"
)

// Envelope 是总线上流转的消息信封，携带 schema 与版本信息。
type Envelope struct {
	ID       string // 消息唯一 ID（同一消息的重投复用该 ID）
	Topic    string
	Schema   string // 逻辑 schema 名
	Version  uint32 // 该消息编码时使用的 schema 版本
	Encoding string // 编码格式，如 "json"
	Data     []byte
	Seq      uint64 // 总线按主题分配的单调序号（顺序判定依据）
}

// ackVerdict 是订阅者对一次投递的结论。
type ackVerdict int

const (
	verdictAck ackVerdict = iota
	verdictNack
)

// Delivery 是投递给订阅者的一次投递尝试。
// 每条消息对每个订阅者至多有一个“在途” Delivery：Ack 后永不再投，
// Nack 或订阅者重连后会产生 Redelivered=true 的新一次投递。
type Delivery struct {
	Envelope    *Envelope
	SubID       string
	ClientID    string
	Attempt     int
	Redelivered bool
	// Reason 在“投递即通知丢弃”时有效（正常投递为 DropNone）。
	Reason DropReason

	done chan ackVerdict
	once sync.Once
}

func newDelivery(env *Envelope, sub *Subscription, attempt int) *Delivery {
	return &Delivery{
		Envelope:    env,
		SubID:       sub.id,
		ClientID:    sub.clientID,
		Attempt:     attempt,
		Redelivered: attempt > 1,
		done:        make(chan ackVerdict, 1),
	}
}

// Ack 确认消息，确认后永不再投。重复确认无效。
func (d *Delivery) Ack() {
	d.once.Do(func() { d.done <- verdictAck })
}

// Nack 拒绝消息（处理失败），高可靠等级下会按序重投。重复拒绝无效。
func (d *Delivery) Nack() {
	d.once.Do(func() { d.done <- verdictNack })
}

func (d *Delivery) wait() ackVerdict { return <-d.done }

// Handler 是订阅者的投递回调。回调返回后需显式调用 Delivery.Ack/Nack；
// 高可靠等级下，在 Ack 之前订阅者断开，消息将被重投。
type Handler func(ctx context.Context, d *Delivery)

// Codec 负责消息的编码与版本感知解码。
type Codec interface {
	Encode(schema string, v any) (*Envelope, error)
	Decode(env *Envelope) (any, error)
}

// DeliveryOutcome 是一条消息对单个订阅者的投递结论（可解释性依据）。
type DeliveryOutcome struct {
	ClientID  string
	SubID     string
	QoS       QoS
	Delivered bool       // 是否已进入订阅者队列/回调
	Reason    DropReason // 未投递时的确切原因
	Detail    string
}

// PublishReport 汇总一次发布对当前所有在线/保留会话的投递结论。
type PublishReport struct {
	Envelope *Envelope
	Outcomes []DeliveryOutcome
}

// Dropped 返回被丢弃的结论数。
func (r *PublishReport) Dropped() int {
	if r == nil {
		return 0
	}
	n := 0
	for _, o := range r.Outcomes {
		if !o.Delivered {
			n++
		}
	}
	return n
}
