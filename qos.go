package pubsub

// QoS 服务质量等级。
type QoS int

const (
	// QoSLowLatency 低延迟等级：满队列 / 无订阅者时允许丢弃，
	// 丢弃时通过 PublishResult 给出可区分的 DropReason。
	QoSLowLatency QoS = 0
	// QoSReliable 高可靠等级：同一主题上的顺序投递 + 至少一次送达，
	// 发布点对背压做有界等待，队列溢出时返回 KindOverflow 错误，
	// 不静默丢失。
	QoSReliable QoS = 1
)

func (q QoS) valid() bool { return q == QoSLowLatency || q == QoSReliable }

func (q QoS) String() string {
	switch q {
	case QoSLowLatency:
		return "qos-low-latency"
	case QoSReliable:
		return "qos-reliable"
	default:
		return "qos-unknown"
	}
}

// DropReason 可区分的丢弃原因。
type DropReason int

const (
	DropNone         DropReason = iota
	DropQueueFull               // 订阅者队列溢出
	DropNoSubscriber            // 无订阅者
	DropEncodeFailed            // 编码失败
	DropBusClosed               // 总线关闭
)

func (r DropReason) String() string {
	switch r {
	case DropNone:
		return "not-dropped"
	case DropQueueFull:
		return "queue-full"
	case DropNoSubscriber:
		return "no-subscriber"
	case DropEncodeFailed:
		return "encode-failed"
	case DropBusClosed:
		return "bus-closed"
	default:
		return "unknown-drop"
	}
}
