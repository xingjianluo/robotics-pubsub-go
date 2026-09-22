package pubsub

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
)

// Bus 是进程内消息总线，模拟多个发布者与订阅者之间的消息传递。
// 并发模型：每个主题一个“发布漏斗” goroutine，主题上的所有发布在此线性化：
// 分配单调序号、取一致的订阅者快照、按各订阅入队时的 QoS 裁决。
// 因此同主题顺序天然保持；某主题背压只阻塞该主题的发布者，
// 绝不阻塞其他主题的发布者，也不存在总线级锁上的等待。
type Bus struct {
	registry    *SchemaRegistry
	log         Logger
	cfg         config
	closingFlag *atomic.Bool

	mu     sync.RWMutex
	topics map[string]*topicState
	closed bool

	done chan struct{}
	idc  atomic.Uint64
	once sync.Once
}

type topicState struct {
	name    string
	seq     uint64
	epoch   uint64 // 订阅关系版本：增/删/改/重连均递增
	subs    map[string]*Subscription
	inbox   chan *publishCall
	stopped bool
}

type publishCall struct {
	env    *Envelope
	ctx    context.Context
	result chan funnelResult
}

type funnelResult struct {
	report *PublishReport
	err    error
}

// Option 配置总线。
type Option func(*config)

type config struct {
	queueDepth  int
	maxInflight int
	dropPolicy  DropPolicy
	log         Logger
	ratePerSec  float64
	// closingFlag 若非空，Close 在终止任何会话前先置位；供测试日志据此停止触碰 testing.T。
	closingFlag *atomic.Bool
}

// WithQueueDepth 设置每个订阅的有界容量：低延迟占用为“队列长度+在途回调数”，
// 高可靠占用为队列长度（含 1 个在途）。容量耗尽时分别表现为丢弃与背压。
func WithQueueDepth(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.queueDepth = n
		}
	}
}

// WithMaxLowLatencyInflight 设置低延迟订阅同时在处理中的回调上限（默认等于队列深度）。
func WithMaxLowLatencyInflight(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxInflight = n
		}
	}
}

// WithDropPolicy 设置低延迟等级队列满时的丢弃策略。
func WithDropPolicy(p DropPolicy) Option {
	return func(c *config) { c.dropPolicy = p }
}

// WithLogger 替换日志实现。
func WithLogger(l Logger) Option {
	return func(c *config) {
		if l != nil {
			c.log = l
		}
	}
}

// WithLowLatencyRateLimit 为每个低延迟订阅设置每秒令牌限速（<=0 表示不限速）。
func WithLowLatencyRateLimit(perSecond float64) Option {
	return func(c *config) { c.ratePerSec = perSecond }
}

// withClosingFlag 注入关闭标志（测试用）：Close 终止会话前置位。
func withClosingFlag(f *atomic.Bool) Option {
	return func(c *config) { c.closingFlag = f }
}

// New 创建总线。
func New(reg *SchemaRegistry, opts ...Option) *Bus {
	cfg := config{queueDepth: 256, dropPolicy: DropNewest, log: defaultLogger()}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.maxInflight == 0 {
		cfg.maxInflight = cfg.queueDepth // 默认与队列深度一致
	}
	b := &Bus{
		registry:    reg,
		log:         cfg.log,
		cfg:         cfg,
		closingFlag: cfg.closingFlag,
		topics:      make(map[string]*topicState),
		done:        make(chan struct{}),
	}
	return b
}

// Subscribe 建立订阅；相同 (clientID, topic) 重新接入时恢复原会话：
// 未确认/未投递的高可靠消息继续按序投递，已确认消息不会重复。
func (b *Bus) Subscribe(clientID, topic string, qos QoS, h Handler) (*Subscription, error) {
	if clientID == "" || topic == "" || h == nil {
		return nil, wrapError(KindInvalidArgument, "subscribe", topic,
			"clientID/topic must be non-empty and handler non-nil", nil)
	}
	if qos != QoSLowLatency && qos != QoSReliable {
		return nil, wrapError(KindInvalidArgument, "subscribe", topic, "unknown qos", nil)
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrBusClosed
	}
	ts := b.topics[topic]
	if ts == nil {
		ts = &topicState{
			name: topic, subs: make(map[string]*Subscription),
			inbox: make(chan *publishCall, b.cfg.queueDepth),
		}
		b.topics[topic] = ts
		go b.runFunnel(ts)
	}
	key := clientID + "#" + topic
	var s *Subscription
	if existing := ts.subs[key]; existing != nil {
		s = existing
		if err := s.connect(h); err != nil {
			b.mu.Unlock()
			return nil, err
		}
		ts.epoch++
		epoch := ts.epoch
		b.mu.Unlock()
		b.log.Printf("[relation] topic=%s client=%s resumed epoch(sub)=%d topicEpoch=%d",
			topic, clientID, s.Epoch(), epoch)
		return s, nil
	}
	s = newSubscription(key, clientID, topic, qos, b.cfg.queueDepth, b.cfg.maxInflight,
		h, b.log, b.done,
		newRateLimiter(b.cfg.ratePerSec))
	if err := s.connect(h); err != nil {
		b.mu.Unlock()
		return nil, err
	}
	ts.subs[key] = s
	ts.epoch++
	epoch := ts.epoch
	b.mu.Unlock()

	go s.run()
	b.log.Printf("[relation] topic=%s client=%s subscribed qos=%s topicEpoch=%d",
		topic, clientID, qos, epoch)
	return s, nil
}

// Publish 向主题发布一条已编码信封。信封的 Topic/ID/Seq 由总线补齐。
// 阻塞语义：若所有/部分订阅为高可靠且队列满，等待它们腾出容量或 ctx 超时；
// 其它主题的发布者与低延迟订阅不受影响。
func (b *Bus) Publish(ctx context.Context, env *Envelope) (*PublishReport, error) {
	if env == nil {
		return nil, wrapError(KindInvalidArgument, "publish", "", "nil envelope", nil)
	}
	inbox, err := b.topicInbox(env.Topic)
	if err != nil {
		return nil, err
	}

	if env.ID == "" {
		env.ID = b.newMsgID()
	}
	call := &publishCall{env: env, ctx: ctx, result: make(chan funnelResult, 1)}
	select {
	case inbox <- call:
	case <-ctx.Done():
		return nil, wrapError(KindBackpressure, "publish", env.Topic,
			"topic funnel busy and context canceled", ctx.Err())
	case <-b.done:
		return nil, ErrBusClosed
	}
	select {
	case r := <-call.result:
		return r.report, r.err
	case <-ctx.Done():
		return nil, wrapError(KindBackpressure, "publish", env.Topic,
			"delivery pending and context canceled", ctx.Err())
	case <-b.done:
		return nil, ErrBusClosed
	}
}

// PublishValue 用注册表编码结构体后发布；编码错误以可区分的 *Error 返回。
func (b *Bus) PublishValue(ctx context.Context, topic, schema string, v any) (*PublishReport, error) {
	if b.registry == nil {
		return nil, wrapError(KindInvalidArgument, "publish", topic, "no schema registry configured", nil)
	}
	env, err := b.registry.Encode(schema, v)
	if err != nil {
		return nil, err
	}
	env.Topic = topic
	return b.Publish(ctx, env)
}

// UpdateQoS 修改订阅的服务质量等级：对下一条发布立即可见（主题发布线性化保证一致）；
// 已在队消息保持入队时盖戳的等级。
func (b *Bus) UpdateQoS(clientID, topic string, qos QoS) error {
	if qos != QoSLowLatency && qos != QoSReliable {
		return wrapError(KindInvalidArgument, "update-qos", topic, "unknown qos", nil)
	}
	s, ts, err := b.lookup(clientID, topic)
	if err != nil {
		return err
	}
	s.setQoS(qos)
	b.mu.Lock()
	ts.epoch++
	b.mu.Unlock()
	return nil
}

// Detach 使订阅者短暂离线但保留会话：未确认消息不丢，重连后按序重投。
func (b *Bus) Detach(clientID, topic string) error {
	s, ts, err := b.lookup(clientID, topic)
	if err != nil {
		return err
	}
	s.detach()
	b.mu.Lock()
	ts.epoch++
	b.mu.Unlock()
	return nil
}

// Unsubscribe 删除订阅关系；deleteSession=false 等价于 Detach（保留会话待续传），
// deleteSession=true 时会话与队列一并清除，旧消息不再投递。
func (b *Bus) Unsubscribe(clientID, topic string, deleteSession bool) error {
	s, ts, err := b.lookup(clientID, topic)
	if err != nil {
		return err
	}
	if !deleteSession {
		s.detach()
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrBusClosed
	}
	delete(ts.subs, s.id)
	ts.epoch++
	b.mu.Unlock()
	s.terminate()
	b.log.Printf("[relation] topic=%s client=%s session deleted topicEpoch=%d",
		topic, clientID, b.topicEpoch(topic))
	return nil
}

// SubscriptionStats 返回某订阅会话的判定计数快照（测试/可观测用）。
func (b *Bus) SubscriptionStats(clientID, topic string) (SubStats, error) {
	s, _, err := b.lookup(clientID, topic)
	if err != nil {
		return SubStats{}, err
	}
	return s.Stats(), nil
}

// TopicEpoch 返回主题订阅关系版本号（订阅增删改均递增）。
func (b *Bus) TopicEpoch(topic string) uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if ts := b.topics[topic]; ts != nil {
		return ts.epoch
	}
	return 0
}

func (b *Bus) topicEpoch(topic string) uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[topic].epoch
}

func (b *Bus) topicInbox(topic string) (chan *publishCall, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrBusClosed
	}
	ts := b.topics[topic]
	if ts == nil {
		ts = &topicState{
			name: topic, subs: make(map[string]*Subscription),
			inbox: make(chan *publishCall, b.cfg.queueDepth),
		}
		b.topics[topic] = ts
		go b.runFunnel(ts)
	}
	return ts.inbox, nil
}

func (b *Bus) lookup(clientID, topic string) (*Subscription, *topicState, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, nil, ErrBusClosed
	}
	ts := b.topics[topic]
	if ts == nil {
		return nil, nil, wrapError(KindInvalidArgument, "lookup", topic,
			"no such topic: "+topic, ErrSubNotFound)
	}
	s := ts.subs[clientID+"#"+topic]
	if s == nil {
		return nil, nil, wrapError(KindInvalidArgument, "lookup", topic,
			"no subscription for client: "+clientID, ErrSubNotFound)
	}
	return s, ts, nil
}

// runFunnel 是主题上唯一的发布线性化点。
func (b *Bus) runFunnel(ts *topicState) {
	for {
		select {
		case call := <-ts.inbox:
			call.result <- b.applyPublish(ts, call)
		case <-b.done:
			return
		}
	}
}

func (b *Bus) applyPublish(ts *topicState, call *publishCall) funnelResult {
	env := call.env
	env.Topic = ts.name

	b.mu.RLock()
	ts.seq++
	env.Seq = ts.seq
	snapshot := make([]*Subscription, 0, len(ts.subs))
	for _, s := range ts.subs {
		snapshot = append(snapshot, s)
	}
	epoch := ts.epoch
	b.mu.RUnlock()

	outcomes := make([]DeliveryOutcome, 0, len(snapshot))
	var firstErr error
	for _, s := range snapshot {
		o := DeliveryOutcome{ClientID: s.clientID, SubID: s.id}
		qos := s.QoS()
		o.QoS = qos
		if qos == QoSReliable {
			if err := s.enqueueReliable(call.ctx, env); err != nil {
				o.Delivered = false
				o.Reason = DropSubscriptionClosed
				o.Detail = err.Error()
				if firstErr == nil {
					firstErr = err
				}
			} else {
				o.Delivered = true
			}
		} else {
			reason := s.enqueueLowLatency(env, b.cfg.dropPolicy)
			o.Reason = reason
			o.Delivered = reason == DropNone
			if !o.Delivered {
				o.Detail = "dropped: " + reason.String()
			}
		}
		outcomes = append(outcomes, o)
	}

	b.log.Printf("[publish] topic=%s seq=%d id=%s schema=%s/v%d recipients=%d dropped=%d topicEpoch=%d",
		ts.name, env.Seq, env.ID, env.Schema, env.Version, len(outcomes),
		countDropped(outcomes), epoch)

	return funnelResult{
		report: &PublishReport{Envelope: env, Outcomes: outcomes},
		err:    firstErr,
	}
}

func countDropped(os []DeliveryOutcome) int {
	n := 0
	for _, o := range os {
		if !o.Delivered {
			n++
		}
	}
	return n
}

// Close 关闭总线：拒绝新发布，终止全部会话，派发/漏斗 goroutine 退出。
func (b *Bus) Close() error {
	b.once.Do(func() {
		// 先置位关闭标志：任何可能仍在运行的回调日志此后只写内存缓冲，
		// 不再触碰 testing.T（生产 logger 无此标志，无影响）。
		if b.closingFlag != nil {
			b.closingFlag.Store(true)
		}
		b.mu.Lock()
		b.closed = true
		var all []*Subscription
		for _, ts := range b.topics {
			for _, s := range ts.subs {
				all = append(all, s)
			}
		}
		b.mu.Unlock()
		close(b.done)
		for _, s := range all {
			s.terminate()
		}
		b.log.Printf("[bus] closed, terminated sessions=%d", len(all))
	})
	return nil
}

func (b *Bus) newMsgID() string {
	n := b.idc.Add(1)
	var raw [4]byte
	_, _ = rand.Read(raw[:])
	return "msg-" + uintToAtoi(n) + "-" + hex.EncodeToString(raw[:])
}

func uintToAtoi(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
