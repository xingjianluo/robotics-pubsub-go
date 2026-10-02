package pubsub

import (
	"time"
)

// GroupConfig 多执行器分担组（competing executors）配置。
//
// 同一个主题下的一个分担组内可以有多个执行器（Executor）同时在线，
// 组内每条消息同一时刻只交给其中一个执行器处理（competing consumers）。
type GroupConfig struct {
	// MaxBacklog 组内保留消息上限（排队中 + 在途未 Ack 合计）。
	// 新发布超过上限时：可靠发布返回 KindOverflow，低延迟发布在
	// PublishResult.Dropped 给出 DropQueueFull；两种情况下消息都进入
	// 该组死信队列，可查看、可重新投递。
	MaxBacklog int
	// ExecutorQueueLen 每个执行器在途通道容量（0 用总线默认值 16）。
	ExecutorQueueLen int
	// ProcessTimeoutMs 消息被某执行器接手后的处理超时。
	// 超时未 Ack 则收回重派给其他在线执行器（至少一次）；
	// 累计接手次数超过 MaxAttempts 仍未 Ack，则进入死信队列
	// （DeadProcessTimeout）。
	ProcessTimeoutMs int
	// WaitTimeoutMs 消息排队等待“被任一执行器接手”的上限。
	// 超时仍无执行器接手则进入死信队列（DeadWaitTimeout），
	// 与处理超时、积压溢出彼此可区分。
	WaitTimeoutMs int
	// MaxAttempts 单条消息最多被派发到执行器的次数（含重派）。
	// 超过后处理仍未 Ack 则进入死信队列（DeadProcessTimeout）。
	MaxAttempts int
}

// ExecutorOption 执行器加入/重入分担组的参数。
type ExecutorOption struct {
	// QueueLen 该执行器在途通道容量（0 用组/总线默认值）。
	QueueLen int
	// ResumeID 非空时以既有执行器身份重新接入（掉线回来接着干）；
	// 为空表示新加入一个执行器。
	ResumeID string
}

// DeadReason 消息进入死信队列的可区分原因。
type DeadReason int

const (
	DeadNone DeadReason = iota
	// DeadBacklogOverflow 组内积压超过 MaxBacklog。
	DeadBacklogOverflow
	// DeadWaitTimeout 排队等待执行器接手超过 WaitTimeoutMs。
	DeadWaitTimeout
	// DeadProcessTimeout 多次接手后处理仍超时（达到 MaxAttempts）。
	DeadProcessTimeout
)

func (r DeadReason) String() string {
	switch r {
	case DeadBacklogOverflow:
		return "dead-backlog-overflow"
	case DeadWaitTimeout:
		return "dead-wait-timeout"
	case DeadProcessTimeout:
		return "dead-process-timeout"
	default:
		return "dead-none"
	}
}

// DeadLetter 死信记录：最终未能投递成功的消息及其可区分原因。
type DeadLetter struct {
	Group    string
	Topic    string
	Key      string
	Reason   DeadReason
	Attempts int
	DeadAt   time.Time
	Msg      *Message

	// qm 指向保留的消息本体，供 RequeueDead 原序重投（内部使用）。
	qm *queuedGroupMsg
}

// GroupStats 分担组运行态快照（判定/复盘依据）。
type GroupStats struct {
	OnlineExecutors int // 在线执行器数
	Retained        int // 组内保留（排队 + 在途未 Ack）消息数
	InFlight        int // 已派发到执行器但未 Ack 的消息数
	DeadLetters     int // 死信队列长度
	PerKey          map[string]int
}

// Executor 分担组中的一个执行器句柄。
//
// 生命周期：Join(active) ──Leave──▶ offline ──Join(ResumeID)──▶ active ... ──Close──▶ removed
type Executor struct {
	id        string
	topic     string
	groupName string
	st        *groupState
	bus       *Bus
}

// ID 返回执行器稳定 ID（掉线重入时复用）。
func (e *Executor) ID() string { return e.id }

// Topic 返回主题。
func (e *Executor) Topic() string { return e.topic }

// Group 返回所属分担组名。
func (e *Executor) Group() string { return e.groupName }

// ExecutorGroup 同一主题下的一个多执行器分担组。
type ExecutorGroup struct {
	topic string
	name  string
	bus   *Bus
	st    *groupState
}

// Topic 返回主题。
func (g *ExecutorGroup) Topic() string { return g.topic }

// Name 返回分担组名。
func (g *ExecutorGroup) Name() string { return g.name }
