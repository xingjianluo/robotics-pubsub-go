package pubsub

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// GroupOption 执行器组（消费者组）参数。
type GroupOption struct {
	// MemberID 非空时以既有成员身份重新入组（续传未确认工作）；
	// 为空表示新建成员。
	MemberID string
	// QueueLen 成员投递通道容量（0 表示用总线默认值）。
	QueueLen int
	// MaxPending 组级积压上限（未确认消息总数），0 表示用总线默认值。
	// 仅在首次建组时生效。
	MaxPending int
	// DeliveryTimeoutMs 消息入组后超过该时长仍未被确认则判失败，
	// 进入死信队列；0 表示用总线默认值。仅在首次建组时生效。
	DeliveryTimeoutMs int
	// MaxAttempts 单条消息最大投递尝试次数，超过判失败进死信队列；
	// 0 表示用总线默认值。仅在首次建组时生效。
	MaxAttempts int
}

// DeadReason 死信（投递最终失败）原因，彼此可区分。
type DeadReason int

const (
	DeadNone        DeadReason = iota
	DeadTimeout                // 等待处理超时仍未被确认
	DeadMaxAttempts            // 投递尝试次数超过上限
)

func (r DeadReason) String() string {
	switch r {
	case DeadTimeout:
		return "delivery-timeout"
	case DeadMaxAttempts:
		return "max-attempts-exceeded"
	default:
		return "not-dead"
	}
}

// DeadLetter 最终未能投递成功的消息记录（可查看、可重新投递）。
type DeadLetter struct {
	Topic    string
	Group    string
	ID       string
	Seq      uint64
	Key      string
	Version  Version
	Payload  []byte
	Reason   DeadReason
	Attempts int
	FailedAt time.Time
}

// GroupMember 执行器句柄：组内一个工作成员。
//
// 生命周期：JoinGroup(online) --Leave--> offline --JoinGroup(MemberID)--> online
// ... --Close--> removed。offline 期间其负责键的未确认消息会转给在线成员，
// 或在没有在线成员时留在组内等它（或任意成员）回来接着处理。
type GroupMember struct {
	id    string
	topic string
	group string
	gs    *groupState
	bus   *Bus
}

// keyQueue 单个顺序键（机器人/任务）的未确认消息队列。
// 只有队首允许在途（head-of-line），保证同键消息严格按序、
// 且任一时刻同一条消息只被一个成员接手。
type keyQueue struct {
	seqs     []uint64 // 未确认序号，升序
	owner    string   // 当前负责成员 ID（"" 表示待分配）
	inFlight bool     // 队首已投递未确认
}

// groupMsg 组级未确认消息。
type groupMsg struct {
	env        *Envelope
	key        string
	attempts   int
	enqueuedAt time.Time
}

// memberState 组成员运行态。
type memberState struct {
	id      string
	online  bool
	closed  bool
	queueN  int
	guard   *chGuard
	leaveCh chan struct{}  // 离线/关闭时关闭，用于解除调度器阻塞发送
	sendWg  sync.WaitGroup // 在途发送计数：关闭通道前等待其归零（避免 close/send 竞态）
}

// groupState 执行器组运行态。
type groupState struct {
	topic string
	name  string

	maxPending  int
	timeoutMs   int
	maxAttempts int

	mu      sync.Mutex
	waitCh  chan struct{}
	members map[string]*memberState
	pending map[uint64]*groupMsg // seq -> 未确认消息
	keys    map[string]*keyQueue // key -> 顺序队列
	highSeq uint64
	dlq     []DeadLetter
	dlqSet  map[uint64]bool
	closed  bool
}

// signalLocked 唤醒组调度协程（与 subState 相同的 channel-Cond 协议）。
func (g *groupState) signalLocked() {
	if g.waitCh != nil {
		close(g.waitCh)
		g.waitCh = make(chan struct{})
	}
}

// enqueueLocked 发布侧把消息挂入组（调用方持 g.mu）。
// 同主题发布在主题锁下串行，seqs 追加天然升序。
func (g *groupState) enqueueLocked(env *Envelope) {
	g.pending[env.Seq] = &groupMsg{env: env, key: env.Key, enqueuedAt: time.Now()}
	kq := g.keys[env.Key]
	if kq == nil {
		kq = &keyQueue{}
		g.keys[env.Key] = kq
	}
	kq.seqs = append(kq.seqs, env.Seq)
	if env.Seq > g.highSeq {
		g.highSeq = env.Seq
	}
	g.signalLocked()
}

// inflight 组内未确认消息总数（积压判定依据）。
func (g *groupState) inflight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.pending)
}

// removeLocked 从 pending 与其键队列中移除序号（调用方持锁）。
// 移除队首时解除 inFlight，后续消息可继续投递；已删除键队列被回收。
func (g *groupState) removeLocked(seq uint64) *groupMsg {
	gm, ok := g.pending[seq]
	if !ok {
		return nil
	}
	delete(g.pending, seq)
	if kq := g.keys[gm.key]; kq != nil {
		for i, s := range kq.seqs {
			if s == seq {
				kq.seqs = append(kq.seqs[:i], kq.seqs[i+1:]...)
				if i == 0 {
					kq.inFlight = false
				}
				break
			}
		}
		if len(kq.seqs) == 0 {
			delete(g.keys, gm.key)
		}
	}
	return gm
}

// toDeadLocked 把一条未确认消息判死并移入死信队列（调用方持锁）。
func (g *groupState) toDeadLocked(seq uint64, reason DeadReason) {
	gm := g.pending[seq]
	if gm == nil {
		return
	}
	g.removeLocked(seq)
	g.dlq = append(g.dlq, DeadLetter{
		Topic:    g.topic,
		Group:    g.name,
		ID:       gm.env.ID,
		Seq:      seq,
		Key:      gm.key,
		Version:  gm.env.Version,
		Payload:  gm.env.Body,
		Reason:   reason,
		Attempts: gm.attempts,
		FailedAt: time.Now(),
	})
	g.dlqSet[seq] = true
	g.signalLocked()
}

// JoinGroup 加入（或创建）主题上的执行器组。
//
//   - opt.MemberID 为空：新建成员入组；组不存在时按 opt 中的容量/超时
//     配置创建组。
//   - opt.MemberID 非空：以既有成员身份重新入组，未确认工作续传；
//     已 Close 的成员不可恢复。
//
// 组内可靠消息按 Key 哈希分给在线成员，任一时刻同一条消息只由一个
// 成员接手；同 Key 消息严格按主题序号顺序投递。
func (b *Bus) JoinGroup(_ context.Context, topic, group string, opt GroupOption) (*GroupMember, error) {
	if topic == "" || group == "" {
		return nil, newError(KindInvalidArgument, topic, "empty topic or group", nil)
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
	gs := b.groups[topic][group]
	if gs == nil {
		gs = b.newGroupStateLocked(topic, group, opt)
		if b.groups[topic] == nil {
			b.groups[topic] = map[string]*groupState{}
		}
		b.groups[topic][group] = gs
	}

	var ms *memberState
	gs.mu.Lock()
	if gs.closed {
		gs.mu.Unlock()
		b.mu.Unlock()
		return nil, newError(KindClosed, topic, "group already closed", nil)
	}
	if opt.MemberID != "" {
		ms = gs.members[opt.MemberID]
		if ms == nil || ms.closed {
			gs.mu.Unlock()
			b.mu.Unlock()
			return nil, newError(KindInvalidArgument, topic,
				"cannot rejoin: member not found or closed: "+opt.MemberID, nil)
		}
		// 重新上线：重建投递通道与离线解除点，旧通道上滞留的副本随通道关闭。
		ms.online = true
		ms.queueN = queueN
		old := ms.guard.replace(make(chan *Delivery, queueN))
		ms.leaveCh = make(chan struct{})
		gs.signalLocked()
		gs.mu.Unlock()
		ms.sendWg.Wait()
		safeCloseCh(old)
	} else {
		n := atomic.AddUint64(&b.idCounter, 1)
		ms = &memberState{
			id:      fmt.Sprintf("%s/m-%d", group, n),
			queueN:  queueN,
			guard:   &chGuard{ch: make(chan *Delivery, queueN)},
			leaveCh: make(chan struct{}),
		}
		ms.online = true
		gs.members[ms.id] = ms
		gs.signalLocked()
		gs.mu.Unlock()
	}
	b.mu.Unlock()

	return &GroupMember{id: ms.id, topic: topic, group: group, gs: gs, bus: b}, nil
}

func (b *Bus) newGroupStateLocked(topic, group string, opt GroupOption) *groupState {
	maxP := opt.MaxPending
	if maxP <= 0 {
		maxP = b.cfg.GroupMaxPending
	}
	timeout := opt.DeliveryTimeoutMs
	if timeout <= 0 {
		timeout = b.cfg.GroupDeliveryTimeoutMs
	}
	maxAtt := opt.MaxAttempts
	if maxAtt <= 0 {
		maxAtt = b.cfg.GroupMaxAttempts
	}
	gs := &groupState{
		topic:       topic,
		name:        group,
		maxPending:  maxP,
		timeoutMs:   timeout,
		maxAttempts: maxAtt,
		waitCh:      make(chan struct{}),
		members:     map[string]*memberState{},
		pending:     map[uint64]*groupMsg{},
		keys:        map[string]*keyQueue{},
		dlqSet:      map[uint64]bool{},
	}
	go b.groupLoop(gs)
	return gs
}

// ID 返回成员 ID（重新入组续传时复用）。
func (m *GroupMember) ID() string { return m.id }

// Group 返回所属组名。
func (m *GroupMember) Group() string { return m.group }

// Topic 返回所属主题。
func (m *GroupMember) Topic() string { return m.topic }

// Messages 返回当前投递通道；重新入组后通道会重建，调用方应重新读取。
func (m *GroupMember) Messages() <-chan *Delivery {
	if m.gs == nil {
		return nil
	}
	m.gs.mu.Lock()
	defer m.gs.mu.Unlock()
	if ms := m.gs.members[m.id]; ms != nil {
		return ms.guard.get()
	}
	return nil
}

// Ack 确认一条消息：从组内未确认集合永久移除，保证不再投递；
// 重复 Ack 幂等。只有队首消息可能被投递，因此 Ack 通常针对队首；
// 对不在未确认集合中的序号视为幂等空操作。
func (m *GroupMember) Ack(seq uint64) error {
	g := m.gs
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return newError(KindClosed, m.topic, "group closed", nil)
	}
	if ms := g.members[m.id]; ms == nil || ms.closed {
		return newError(KindInvalidArgument, m.topic, "member closed", nil)
	}
	g.removeLocked(seq)
	g.signalLocked()
	return nil
}

// Leave 暂时离线：本成员负责键的未确认消息立即变为可重分配，
// 由调度器转给仍在线的成员；没有在线成员时留在组内等待。
// 已确认的消息不受影响。成员状态保留，可用 JoinGroup(MemberID) 回来。
func (m *GroupMember) Leave(_ context.Context) error {
	g := m.gs
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return newError(KindClosed, m.topic, "group closed", nil)
	}
	ms := g.members[m.id]
	if ms == nil || ms.closed {
		g.mu.Unlock()
		return newError(KindInvalidArgument, m.topic, "member closed", nil)
	}
	if !ms.online {
		g.mu.Unlock()
		return nil
	}
	ms.online = false
	safeCloseLeaveCh(ms.leaveCh) // 解除调度器对本成员通道的阻塞发送
	// 重建投递通道：旧通道中滞留的未确认副本随通道关闭作废，
	// 它们会被调度器转派给在线成员（at-least-once，消费方按 Seq 去重）。
	old := ms.guard.replace(make(chan *Delivery, ms.queueN))
	// 释放本成员名下的键：未确认工作可转给在线成员。
	for _, kq := range g.keys {
		if kq.owner == m.id {
			kq.owner = ""
			kq.inFlight = false
		}
	}
	g.signalLocked()
	g.mu.Unlock()
	// leaveCh 已关闭，在途发送会立即解除；等其归零后再关旧通道，
	// 保证没有发送协程还在使用该通道。
	ms.sendWg.Wait()
	safeCloseCh(old)
	return nil
}

// Close 永久退出组并销毁成员状态（之后不可再以此 ID 入组）。
func (m *GroupMember) Close() error {
	g := m.gs
	g.mu.Lock()
	ms := g.members[m.id]
	if ms == nil || ms.closed {
		g.mu.Unlock()
		return nil
	}
	ms.closed = true
	ms.online = false
	safeCloseLeaveCh(ms.leaveCh)
	delete(g.members, m.id)
	for _, kq := range g.keys {
		if kq.owner == m.id {
			kq.owner = ""
			kq.inFlight = false
		}
	}
	ch := ms.guard.get()
	g.signalLocked()
	g.mu.Unlock()
	ms.sendWg.Wait()
	safeCloseCh(ch)
	return nil
}

// DeadLetters 查看组的死信队列（最终未能投递成功的消息）。
func (b *Bus) DeadLetters(topic, group string) ([]DeadLetter, error) {
	g := b.groupState(topic, group)
	if g == nil {
		return nil, newError(KindInvalidArgument, topic, "group not found: "+group, nil)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]DeadLetter, len(g.dlq))
	copy(out, g.dlq)
	return out, nil
}

// RedeliverDeadLetter 把一条死信重新投递回组：
// 以原序号插回其键队列的升序位置（顺序保持），attempts 重新计数；
// 若该序号当前已在未确认集合中（不可能同时死信）则拒绝，保证不重复。
func (b *Bus) RedeliverDeadLetter(topic, group string, seq uint64) error {
	g := b.groupState(topic, group)
	if g == nil {
		return newError(KindInvalidArgument, topic, "group not found: "+group, nil)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return newError(KindClosed, topic, "group closed", nil)
	}
	if !g.dlqSet[seq] {
		return newError(KindInvalidArgument, topic,
			fmt.Sprintf("seq=%d not in dead letter queue", seq), nil)
	}
	if _, ok := g.pending[seq]; ok {
		return newError(KindInvalidArgument, topic,
			fmt.Sprintf("seq=%d already pending", seq), nil)
	}
	var dl DeadLetter
	for i, d := range g.dlq {
		if d.Seq == seq {
			dl = d
			g.dlq = append(g.dlq[:i], g.dlq[i+1:]...)
			break
		}
	}
	delete(g.dlqSet, seq)
	env := &Envelope{
		ID: dl.ID, Topic: dl.Topic, Version: dl.Version,
		Seq: dl.Seq, Key: dl.Key, Body: dl.Payload,
	}
	g.pending[seq] = &groupMsg{env: env, key: dl.Key, enqueuedAt: time.Now()}
	kq := g.keys[dl.Key]
	if kq == nil {
		kq = &keyQueue{}
		g.keys[dl.Key] = kq
	}
	// 升序插入：重新投递的旧消息排在其键内更新消息之前。
	pos := sort.Search(len(kq.seqs), func(i int) bool { return kq.seqs[i] > seq })
	kq.seqs = append(kq.seqs, 0)
	copy(kq.seqs[pos+1:], kq.seqs[pos:])
	kq.seqs[pos] = seq
	g.signalLocked()
	return nil
}

func (b *Bus) groupState(topic, group string) *groupState {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.groups[topic][group]
}
