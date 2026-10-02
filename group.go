package pubsub

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// queuedGroupMsg 分担组内保留的一条消息。
type queuedGroupMsg struct {
	env         *Envelope
	key         string
	attempts    int         // 已被派发到执行器的次数（含重派）
	leasedBy    string      // 当前接手执行器 ID（空表示可被在线执行器接手）
	enqueuedAt  time.Time   // 首次进入组内保留队列的时间（等待超时判定）
	lastLeaseAt time.Time   // 最近一次被接手时间（调试/复盘依据）
	timer       *time.Timer // 当前处理超时看门狗（仅在派发后非 nil）
}

// execState 分担组内单个执行器的运行态。
type execState struct {
	id       string
	guard    *chGuard // 当前投递通道（ResumeID 重入时整体替换）
	queueN   int
	inflight int // 已派发给该执行器但未 Ack 的消息数（在途窗口判定依据）
	online   bool
	closed   bool
}

// groupState 单个 (topic, group) 的运行态。
//
// 所有调度决策只在单把 mu 下完成；组级派发协程（groupDispatcher）
// 是唯一向执行器通道发送的一方，因此“选活 → 标记租约 → 发送”无需
// 跨协程竞争。
type groupState struct {
	topic  string
	name   string
	cfg    GroupConfig
	codec  Codec
	schema Schema

	mu sync.Mutex

	// keyOrder 保留消息进入时的 Key 首次出现顺序（稳定遍历/调试）。
	keyOrder []string
	// perKey：每个 Key 一条严格有序队列；队列中某条带租约时，
	// 同 Key 后续消息一律等待，保证同 Key 顺序不被换执行器打乱。
	perKey map[string][]*queuedGroupMsg
	// index seq -> 消息本体（Ack/重投/死信定位）。
	index map[uint64]*queuedGroupMsg

	// executors id -> state；onlineIDs 为在线执行器的稳定轮转顺序。
	executors map[string]*execState
	onlineIDs []string
	rrCursor  int

	// keyRR 派发扫描 Key 队列时的公平轮转起点。
	keyRR int

	dead []*DeadLetter

	closed bool
	waitCh chan struct{} // 状态变化广播点（channel-Cond，同 subState 协议）
}

// newGroupState 构造分担组运行态。
func newGroupState(topic, name string, cfg GroupConfig, schema Schema, codec Codec) *groupState {
	if cfg.MaxBacklog <= 0 {
		cfg.MaxBacklog = 128
	}
	if cfg.ExecutorQueueLen <= 0 {
		cfg.ExecutorQueueLen = 16
	}
	if cfg.ProcessTimeoutMs <= 0 {
		cfg.ProcessTimeoutMs = 5000
	}
	if cfg.WaitTimeoutMs <= 0 {
		cfg.WaitTimeoutMs = 30000
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	return &groupState{
		topic:     topic,
		name:      name,
		cfg:       cfg,
		codec:     codec,
		schema:    schema,
		perKey:    map[string][]*queuedGroupMsg{},
		index:     map[uint64]*queuedGroupMsg{},
		executors: map[string]*execState{},
		waitCh:    make(chan struct{}),
	}
}

// signalLocked 唤醒派发协程（调用方持 mu）。
// 与 subState 相同的 channel-Cond 协议：持锁 close 当前等待点并换新，
// 派发协程持锁安装等待点后才解锁等待，信号不存在丢失窗口。
func (gs *groupState) signalLocked() {
	if gs.waitCh != nil {
		close(gs.waitCh)
		gs.waitCh = make(chan struct{})
	}
}

// retainedLocked 组内保留消息总数（排队 + 在途未 Ack）。
func (gs *groupState) retainedLocked() int { return len(gs.index) }

// nextExecLocked 从在线执行器轮转列表中选出下一个候选（公平轮转）。
func (gs *groupState) nextExecLocked() (*execState, bool) {
	n := len(gs.onlineIDs)
	if n == 0 {
		return nil, false
	}
	gs.rrCursor %= n
	id := gs.onlineIDs[gs.rrCursor]
	gs.rrCursor = (gs.rrCursor + 1) % n
	return gs.executors[id], true
}

// freeExecLocked 找出一个在途窗口未满的在线执行器（round-robin）。
func (gs *groupState) freeExecLocked() *execState {
	n := len(gs.onlineIDs)
	for i := 0; i < n; i++ {
		id := gs.onlineIDs[(gs.rrCursor+i)%n]
		ex := gs.executors[id]
		if ex != nil && ex.online && ex.inflight < ex.queueN {
			gs.rrCursor = (gs.rrCursor + i + 1) % n
			return ex
		}
	}
	return nil
}

// removeOnlineLocked 把执行器移出在线轮转列表。
func (gs *groupState) removeOnlineLocked(id string) {
	for i, x := range gs.onlineIDs {
		if x == id {
			gs.onlineIDs = append(gs.onlineIDs[:i], gs.onlineIDs[i+1:]...)
			if gs.rrCursor >= len(gs.onlineIDs) && len(gs.onlineIDs) > 0 {
				gs.rrCursor %= len(gs.onlineIDs)
			}
			return
		}
	}
}

// pushKeyLocked 把消息追加到其 Key 的有序队列末尾（新 Key 记录首次出现顺序）。
func (gs *groupState) pushKeyLocked(qm *queuedGroupMsg) {
	if _, ok := gs.perKey[qm.key]; !ok {
		gs.keyOrder = append(gs.keyOrder, qm.key)
	}
	gs.perKey[qm.key] = append(gs.perKey[qm.key], qm)
	gs.index[qm.env.Seq] = qm
}

// removeFromKeyLocked 从其 Key 队列中删除消息（调用方已持锁；
// 可能是队头完成，也可能是死信）。返回被删消息。
func (gs *groupState) removeFromKeyLocked(qm *queuedGroupMsg) *queuedGroupMsg {
	q := gs.perKey[qm.key]
	for i, x := range q {
		if x == qm {
			gs.perKey[qm.key] = append(q[:i], q[i+1:]...)
			break
		}
	}
	if len(gs.perKey[qm.key]) == 0 {
		delete(gs.perKey, qm.key)
		for i, k := range gs.keyOrder {
			if k == qm.key {
				gs.keyOrder = append(gs.keyOrder[:i], gs.keyOrder[i+1:]...)
				break
			}
		}
	}
	delete(gs.index, qm.env.Seq)
	return qm
}

// requeueHeadLocked 把（通常来自死信队列的）消息插回其 Key 队列最前：
// 重置派发状态与尝试次数，保证它排在同 Key 现有消息之前被处理。
func (gs *groupState) requeueHeadLocked(qm *queuedGroupMsg) {
	qm.attempts = 0
	qm.leasedBy = ""
	qm.enqueuedAt = time.Now()
	qm.lastLeaseAt = time.Time{}
	if qm.timer != nil {
		qm.timer.Stop()
		qm.timer = nil
	}
	if _, ok := gs.perKey[qm.key]; !ok {
		gs.keyOrder = append([]string{qm.key}, gs.keyOrder...)
	}
	gs.perKey[qm.key] = append([]*queuedGroupMsg{qm}, gs.perKey[qm.key]...)
	gs.index[qm.env.Seq] = qm
}

// statsLocked 取运行态快照。
func (gs *groupState) statsLocked() GroupStats {
	perKey := make(map[string]int, len(gs.perKey))
	inFlight := 0
	for k, q := range gs.perKey {
		perKey[k] = len(q)
		for _, qm := range q {
			if qm.leasedBy != "" {
				inFlight++
			}
		}
	}
	return GroupStats{
		OnlineExecutors: len(gs.onlineIDs),
		Retained:        len(gs.index),
		InFlight:        inFlight,
		DeadLetters:     len(gs.dead),
		PerKey:          perKey,
	}
}

// nextExecutorID 在总线级生成稳定的执行器 ID。
func nextExecutorID(b *Bus) string {
	n := atomic.AddUint64(&b.execIDCounter, 1)
	return fmt.Sprintf("exec-%d", n)
}
