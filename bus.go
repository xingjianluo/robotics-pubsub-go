package pubsub

import (
	"sync"
)

// BusConfig 总线配置。
type BusConfig struct {
	// DefaultQueueLen 订阅者投递通道默认容量（也是可靠等级在途窗口）。
	DefaultQueueLen int
	// ReliableEnqueueTimeoutMs 可靠发布遇到慢订阅者背压时的有界等待上限。
	ReliableEnqueueTimeoutMs int
	// RedeliveryTimeoutMs 在途未确认消息的重发间隔（模拟会话续传）。
	RedeliveryTimeoutMs int
	// GroupMaxPending 执行器组默认积压上限（组内未确认消息总数）。
	GroupMaxPending int
	// GroupDeliveryTimeoutMs 组内消息默认投递超时：入组后超过该时长
	// 仍未被确认则判失败并进入死信队列。
	GroupDeliveryTimeoutMs int
	// GroupMaxAttempts 组内消息默认最大投递尝试次数，超过判失败进死信队列。
	GroupMaxAttempts int
}

// PublishResult 发布结论：低延迟等级丢弃时给出可区分原因。
type PublishResult struct {
	Topic     string
	Seq       uint64
	Delivered int // 已入队的在线订阅者数
	// GroupDelivered 本次发布成功入队的执行器组数（可靠语义）。
	GroupDelivered int
	Dropped        []DropInfo // 丢弃明细（低延迟等级）
}

// DropInfo 单个订阅者的丢弃信息。
type DropInfo struct {
	SubscriptionID string
	Reason         DropReason
}

// queuedMsg 订阅者侧保留的未完成消息。
type queuedMsg struct {
	env       *Envelope
	attempts  int
	inCh      bool // 当前是否还在投递通道中（未被消费者读走）
	delivered bool // 是否已至少一次送入通道（用于重发判断）
}

// subState 单个订阅关系的运行态。
type subState struct {
	id     string
	topic  string
	qos    QoS
	ch     chan *Delivery
	queueN int // 通道容量（背压/溢出判定依据）

	mu           sync.Mutex
	waitCh       chan struct{} // 当前等待点（dispatcher 持锁时创建），signal 关闭它
	guard        *chGuard
	pending      map[uint64]*queuedMsg // seq -> 未 Ack 消息
	highSeq      uint64                // 已入队最大序号
	scheduledSeq uint64                // 已选中送入“当前通道”的连续最大序号（单调，与 Ack 无关）
	minSeq       uint64                // 最小未 Ack 序号（确认水位）
	redeliver    []uint64              // 已读未 Ack、超时请求重发的序号

	offline   bool
	closed    bool
	redelivMs int

	// abortCh 用于解除投递协程在“当前通道”上的阻塞发送：
	// resume/关闭时先关闭它让在途发送退出，再安全关闭旧通道。
	abortCh chan struct{}
	// sendWg 在途发送计数：关闭通道前等待其归零，消除 close/send 竞态。
	sendWg sync.WaitGroup
}

// Bus 进程内消息总线（回环模拟发布订阅链路）。
type Bus struct {
	cfg   BusConfig
	codec Codec
	frame *frameCodec

	mu         sync.RWMutex
	topicLocks map[string]*sync.Mutex            // 每主题发布串行锁
	topics     map[string]map[string]*subState   // topic -> subID -> state
	byID       map[string]*subState              // subID -> state（续传查找）
	groups     map[string]map[string]*groupState // topic -> group -> state
	schemas    map[string]Schema
	seqMu      sync.Mutex
	seqs       map[string]uint64 // topic -> 单调递增序号
	closed     bool

	idCounter uint64
}

// New 创建总线。codec 为 nil 时使用默认 JSONCodec。
func New(cfg BusConfig, codec Codec) *Bus {
	if cfg.DefaultQueueLen <= 0 {
		cfg.DefaultQueueLen = 16
	}
	if cfg.ReliableEnqueueTimeoutMs <= 0 {
		cfg.ReliableEnqueueTimeoutMs = 1000
	}
	if cfg.RedeliveryTimeoutMs <= 0 {
		cfg.RedeliveryTimeoutMs = 2000
	}
	if cfg.GroupMaxPending <= 0 {
		cfg.GroupMaxPending = 256
	}
	if cfg.GroupDeliveryTimeoutMs <= 0 {
		cfg.GroupDeliveryTimeoutMs = 5000
	}
	if cfg.GroupMaxAttempts <= 0 {
		cfg.GroupMaxAttempts = 5
	}
	if codec == nil {
		codec = NewJSONCodec()
	}
	b := &Bus{
		cfg:        cfg,
		codec:      codec,
		frame:      newFrameCodec(),
		topics:     map[string]map[string]*subState{},
		topicLocks: map[string]*sync.Mutex{},
		byID:       map[string]*subState{},
		groups:     map[string]map[string]*groupState{},
		schemas:    map[string]Schema{},
		seqs:       map[string]uint64{},
	}
	return b
}

// topicLock 返回指定主题的发布串行锁（惰性创建）。
// 同一主题的“序号分配 + 投递入队”在此锁下原子完成，
// 从而保证 pending 中序号连续、不会因发布并发而暂时缺号；
// 不同主题使用不同锁，互不阻塞。
func (b *Bus) topicLock(topic string) *sync.Mutex {
	b.mu.Lock()
	defer b.mu.Unlock()
	l, ok := b.topicLocks[topic]
	if !ok {
		l = &sync.Mutex{}
		b.topicLocks[topic] = l
	}
	return l
}

// RegisterSchema 注册主题 schema（解码与版本校验依据）。
func (b *Bus) RegisterSchema(s Schema) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.schemas[s.TopicName()] = s
}

// Codec 返回总线使用的负载编解码器（可用于注册可接受版本等）。
func (b *Bus) Codec() Codec { return b.codec }
