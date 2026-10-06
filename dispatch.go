package pubsub

import (
	"sync"
	"time"
)

// chGuard 保护订阅者当前投递通道（resume 时会整体替换）。
type chGuard struct {
	mu sync.RWMutex
	ch chan *Delivery
}

func (g *chGuard) get() chan *Delivery {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.ch
}

func (g *chGuard) replace(ch chan *Delivery) chan *Delivery {
	g.mu.Lock()
	defer g.mu.Unlock()
	old := g.ch
	g.ch = ch
	return old
}

// enqueue 发布侧入队一条消息（尚未送入通道），返回未完成消息总数。
func (st *subState) enqueue(env *Envelope) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.pending[env.Seq] = &queuedMsg{env: env}
	if env.Seq > st.highSeq {
		st.highSeq = env.Seq
	}
	if len(st.pending) == 1 {
		st.minSeq = env.Seq
	} else if env.Seq < st.minSeq {
		st.minSeq = env.Seq
	}
	st.signalLocked()
	return len(st.pending)
}

// trySendDirect 低延迟等级在线时的尽力投递：不进 pending、不阻塞、不重发。
// 满（select-default 失败）返回 true，由发布侧记录丢弃原因。
func (st *subState) trySendDirect(env *Envelope) (droppedFull bool) {
	st.mu.Lock()
	ch := st.guard.get()
	d := &Delivery{Msg: envelopeToMessage(env), Attempt: 1}
	st.mu.Unlock()
	select {
	case ch <- d:
		return false
	default:
		return true
	}
}

// inflightCount 未完成消息数（未入通道 + 已入通道未 Ack）。
func (st *subState) inflightCount() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.pending)
}

// windowOpen 报告可靠投递窗口是否有空位（未完成数 < 通道容量）。
// 容量与未完成数必须在同一把锁下读取：续传可能并发调整 queueN。
func (st *subState) windowOpen() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.pending) < st.queueN
}

// ack 确认消息：从 pending 永久删除，保证不重复投递。重复 Ack 幂等。
func (st *subState) ack(seq uint64) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return newError(KindInvalidArgument, st.topic, "subscription closed", nil)
	}
	if _, ok := st.pending[seq]; !ok {
		return nil
	}
	delete(st.pending, seq)
	for i, x := range st.redeliver {
		if x == seq {
			st.redeliver = append(st.redeliver[:i], st.redeliver[i+1:]...)
			break
		}
	}
	if seq == st.minSeq {
		st.advanceLocked()
	}
	st.signalLocked()
	return nil
}

// advanceLocked 把 minSeq 推进到新的最小未确认序号。
func (st *subState) advanceLocked() {
	for st.minSeq <= st.highSeq {
		if _, ok := st.pending[st.minSeq]; ok {
			return
		}
		st.minSeq++
	}
}

// resetForResume 重建通道，所有未 Ack 消息重置为未投递，scheduledSeq 归零。
// 返回旧通道与旧代际的发送计数（由调用方等待归零后关闭旧通道），
// 投递协程会在新通道从最小序号重发。
// 同时自增调度代际号 epoch：此前在途的发送醒来后发现代际已变，
// 不得再回滚 scheduledSeq/inCh（状态已在此重排）。
func (st *subState) resetForResume() (chan *Delivery, *sync.WaitGroup) {
	st.mu.Lock()
	defer st.mu.Unlock()
	newCh := make(chan *Delivery, st.queueN)
	old := st.guard.replace(newCh)
	// 更换中止点：旧 abortCh 关闭以解除旧通道上的在途阻塞发送。
	if st.abortCh != nil {
		close(st.abortCh)
	}
	st.abortCh = make(chan struct{})
	// 更换发送计数代际：旧代际的在途发送由调用方等待归零；
	// 新代际的 Add 落在新的 WaitGroup 上，与旧代际的 Wait 不并发。
	oldWg := st.sendWg
	st.sendWg = &sync.WaitGroup{}
	st.epoch++
	for _, qm := range st.pending {
		qm.inCh = false
		qm.delivered = false
	}
	st.redeliver = nil
	st.minSeq = st.smallestPendingLocked()
	// 从“最小未 Ack 序号”开始重投：把调度水位放在它前一个序号，
	// nextReadyLocked 取 scheduledSeq+1 即最小未确认消息；已 Ack 的
	// 低序号不在 pending，也不会被重复投递。
	if len(st.pending) > 0 {
		st.scheduledSeq = st.minSeq - 1
	} else {
		st.scheduledSeq = st.highSeq
	}
	st.offline = false
	st.signalLocked()
	return old, oldWg
}

func (st *subState) smallestPendingLocked() uint64 {
	if len(st.pending) == 0 {
		return st.minSeq
	}
	smallest := ^uint64(0)
	for seq := range st.pending {
		if seq < smallest {
			smallest = seq
		}
	}
	return smallest
}

// requestRedeliveryLocked 把序号加入重发队列（去重），调用方持锁。
func (st *subState) requestRedeliveryLocked(seq uint64) {
	if q, ok := st.pending[seq]; !ok || q.inCh {
		return
	}
	for _, x := range st.redeliver {
		if x == seq {
			return
		}
	}
	st.redeliver = append(st.redeliver, seq)
	st.signalLocked()
}

// nextToSendLocked 返回下一条应发送的消息（调用方持锁）。
// 优先发送正常连续消息（严格保序）；当所有已入队正常消息都已送入
// 通道（scheduledSeq 到达 highSeq）后，才按入队顺序处理重发请求，
// 从而重发副本绝不会插到尚未投递的更新消息之前。
func (st *subState) nextToSendLocked() (uint64, *queuedMsg, bool, bool) {
	if seq, qm, ok := st.nextReadyLocked(); ok {
		return seq, qm, true, false
	}
	// 正常消息全部已送入通道：清理并重发队列。
	for len(st.redeliver) > 0 {
		seq := st.redeliver[0]
		qm, ok := st.pending[seq]
		if !ok {
			// 期间已 Ack。
			st.redeliver = st.redeliver[1:]
			continue
		}
		if qm.inCh {
			// 已在通道（可能刚被 resume/其它路径送入），跳过该项。
			st.redeliver = st.redeliver[1:]
			continue
		}
		st.redeliver = st.redeliver[1:]
		return seq, qm, true, true
	}
	return 0, nil, false, false
}

// nextReadyLocked 返回“下一条允许送入通道”的正常消息（调用方持锁）。
//
// scheduledSeq 是已选中送入当前通道的连续最大序号，单调推进、与 Ack
// 完全无关。候选严格为 scheduledSeq+1，因此：
//   - 低序号一定比高序号更早（重新）进入通道，顺序不乱；
//   - 已 Ack 的序号在它首次发送时就已让 scheduledSeq 越过，Ack 删除
//     pending 不会导致跳号或漏发；
//   - 候选若仍在通道（inCh）则等待空槽（阻塞发送由通道自身消化）。
func (st *subState) nextReadyLocked() (uint64, *queuedMsg, bool) {
	// 从水位之后找第一个“仍未确认且不在通道中”的序号；
	// 已 Ack（pending 中缺失）的序号在其首次发送时就已越过水位，
	// 这里同样跳过并推进水位，避免被已确认空洞卡住。
	for st.scheduledSeq < st.highSeq {
		next := st.scheduledSeq + 1
		qm, ok := st.pending[next]
		if !ok {
			st.scheduledSeq = next // 已 Ack，越过
			continue
		}
		if qm.inCh {
			return 0, nil, false // 队首都在通道，等其离开（阻塞发送/读走）
		}
		return next, qm, true
	}
	return 0, nil, false
}

// dispatchLoop 每个订阅者一个协程，严格按主题序号顺序把消息送入通道。
//
// 两段式，避免任何“通道满 + 通知丢失”死锁，也不要求消费者配合：
//
//  1. 锁内 plan：cond.Wait 原子解锁等待“有下一条待发消息”的状态变化
//     （新入队 / resume / 重发请求）。选中后置 inCh=true 并推进连续
//     水位 scheduledSeq（正常消息），随后解锁；
//  2. 锁外 send：对通道做阻塞发送。通道满时本协程只是挂起且不持锁，
//     消费者可自由读走消息，发送随即完成——无需任何“读槽”信号。
//
// 顺序保证：正常消息只按 scheduledSeq+1 连续选取，高序号不会越过未入
// 通道的低序号；重发请求只在所有正常消息都已送入通道后处理。
// signalLocked 在持有 st.mu 时推进状态版本并唤醒 dispatcher。
// 通过 close 当前 waitCh 实现广播：等待者持锁安装 waitCh 后才解锁
// 等待，因此 signal 无论发生在解锁前还是后，等待者等待的 channel
// 都一定会被关闭，不存在丢失窗口。close 后立刻换新 channel 供下次。
func (st *subState) signalLocked() {
	if st.waitCh != nil {
		close(st.waitCh)
		st.waitCh = make(chan struct{})
	}
}

func (b *Bus) dispatchLoop(st *subState) {
	defer func() {
		// 协程退出（仅发生在订阅关闭时）后允许重新拉起。
		st.mu.Lock()
		st.loopRunning = false
		st.mu.Unlock()
	}()
	for {
		// 锁内决策：取出下一条；无消息可发时按协议在锁内等待。
		st.mu.Lock()
		nextSeq, ch, d, red, redeliv, ep, exitSel := b.selectLocked(st)
		var wg *sync.WaitGroup
		if !exitSel {
			// 锁内登记在途发送（按当前代际的 WaitGroup）：resume/close
			// 关通道前会等待对应代际归零，而它们换代际/置 closed 也在
			// 同一把锁下，不存在“已关闭仍新增发送”的窗口。
			wg = st.sendWg
			wg.Add(1)
		}
		abort := st.abortCh
		st.mu.Unlock()
		if exitSel {
			return
		}

		// 锁外阻塞发送：通道满则挂起但不持锁，消费者可自由读走，
		// 不需要任何“读槽”信号也不会死锁；resume/close 关闭 abortCh
		// 可立即解除本次发送（随后按未送出处理并重排）。
		sent := true
		func() {
			defer func() {
				if r := recover(); r != nil {
					sent = false
				}
			}()
			if abort == nil {
				select {
				case ch <- d:
				default:
					sent = false
				}
				return
			}
			select {
			case ch <- d:
			case <-abort:
				sent = false
			}
		}()

		wg.Done()
		exitNow := false
		st.mu.Lock()
		if redeliv && sent {
			go b.watchRedelivery(st, nextSeq, time.Duration(st.redelivMs)*time.Millisecond)
		}
		switch {
		case st.closed:
			exitNow = true
		case st.epoch != ep:
			// 发送期间发生了续传重置：inCh/scheduledSeq 已被
			// resetForResume 重排，本副本随旧通道作废，会由新一代
			// 调度按序重投。此处绝不能回滚，否则会把水位退到
			// 未投递消息之后（丢消息）或造成乱序。
		case !sent:
			// 同一代际内发送未完成：回滚，让该消息重新被调度。
			if q, ok := st.pending[nextSeq]; ok {
				q.inCh = false
			}
			if !red {
				st.scheduledSeq = nextSeq - 1
			}
		}
		st.mu.Unlock()
		if exitNow {
			return
		}
		// 锁外发送期间到达的新事件已写入 wake；下一轮 selectLocked
		// 在最终阻塞前会先持锁 drain 并复核条件，信号不会被错过。
	}
}

// selectLocked 持锁选出下一条待发消息；没有则以 generation 协议等待：
// 记录当前 gen -> 解锁等 wake -> 回锁比对 gen。期间任何状态变化都
// 会令 gen 自增，因此即使信号被合并也能被发现，不存在丢唤醒窗口。
// 返回的 ep 是选中消息时的调度代际号，供发送完成后校验状态是否仍有效。
func (b *Bus) selectLocked(st *subState) (seq uint64, ch chan *Delivery, d *Delivery, red bool, redeliv bool, ep uint64, exit bool) {
	for {
		if st.closed {
			return 0, nil, nil, false, false, 0, true
		}
		if !st.offline {
			nextSeq, qm, has, isRed := st.nextToSendLocked()
			if has {
				qm.attempts++
				qm.inCh = true
				qm.delivered = true
				if !isRed {
					st.scheduledSeq = nextSeq
				}
				return nextSeq, st.guard.get(),
					&Delivery{Msg: envelopeToMessage(qm.env), Attempt: qm.attempts},
					isRed, st.qos == QoSReliable && st.redelivMs > 0, st.epoch, false
			}
		}
		// 持锁安装本次等待点，然后解锁等待其被关闭。
		w := st.waitCh
		st.mu.Unlock()
		<-w
		st.mu.Lock()
		// 醒来无条件回顶部复核当前状态（容忍虚假/合并唤醒）。
	}
}

// watchRedelivery 可靠等级的在途重发看门狗：消息送入通道后若在 d
// 时间内仍未 Ack，且已离开通道（消费者读走未确认），则把它追加到
// redeliver 队列，调度器在所有更新消息送入通道、有空槽时按序重投
// （at-least-once；消费者据 Seq/Attempt 去重）。不回退 scheduledSeq。
func (b *Bus) watchRedelivery(st *subState, seq uint64, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	<-timer.C
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return
	}
	q, ok := st.pending[seq]
	if !ok || q.inCh {
		return
	}
	st.requestRedeliveryLocked(seq)
}

func envelopeToMessage(env *Envelope) *Message {
	return &Message{
		ID:      env.ID,
		Topic:   env.Topic,
		Version: env.Version,
		Seq:     env.Seq,
		Key:     env.Key,
		Payload: env.Body,
	}
}
