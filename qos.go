package pubsub

// QoS 是订阅者对某一主题要求的服务质量等级。
type QoS int

const (
	// QoSLowLatency 低延迟等级：尽力投递，允许丢弃，但必须给出可区分的丢弃原因。
	QoSLowLatency QoS = 0
	// QoSReliable 高可靠等级：同一主题严格顺序投递、至少一次送达，未确认消息会重投。
	QoSReliable QoS = 1
)

// String 返回 QoS 的可读名称。
func (q QoS) String() string {
	switch q {
	case QoSLowLatency:
		return "low-latency(0)"
	case QoSReliable:
		return "reliable(1)"
	default:
		return "unknown-qos"
	}
}

// DropReason 标识低延迟等级下一条消息被丢弃的确切原因。
type DropReason int

const (
	// DropNone 表示未丢弃。
	DropNone DropReason = iota
	// DropQueueFull 订阅者有界队列已满且策略为丢弃。
	DropQueueFull
	// DropSubscriberOffline 订阅者离线（会话保留但未连接）。
	DropSubscriberOffline
	// DropRateLimited 触发限速保护。
	DropRateLimited
	// DropSubscriptionClosed 订阅关系已被删除。
	DropSubscriptionClosed
)

// String 返回丢弃原因的可读名称。
func (r DropReason) String() string {
	switch r {
	case DropNone:
		return "not-dropped"
	case DropQueueFull:
		return "queue-full"
	case DropSubscriberOffline:
		return "subscriber-offline"
	case DropRateLimited:
		return "rate-limited"
	case DropSubscriptionClosed:
		return "subscription-closed"
	default:
		return "unknown-drop-reason"
	}
}
