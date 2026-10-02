package pubsub

import (
	"context"
	"time"
)

// assignment 不单独存在：组内“选活 -> 打租约 -> 入通道”全部在 gs.mu
// 同一临界区完成，因此无需跨协程的中间结构体。

// groupDispatchLoop 每个分担组一个派发协程，是唯一向执行器通道发送
// 的一方。选活规则：
//
//   - 逐个 Key 取其有序队列队头；队头已带租约（同 Key 在途未完成）时
//     整个 Key 阻塞等待，因此同 Key 消息严格按序、且换执行器也不乱序；
//   - 执行器按在线轮转列表公平（round-robin）分配，谁有空槽派给谁；
//   - 选活与非阻塞入通道在同一把 gs.mu 内完成：空槽由在途计数
//     （inflight < queueN）保证，发送必定立即成功，既不阻塞持锁，
//     也不会出现“租约已打、执行器却已离开”的窗口。
func (b *Bus) groupDispatchLoop(gs *groupState) {
	// 兜底周期唤醒：除 Join/Leave/Ack/超时/入队/Receive 的显式 signal 外，
	// 周期性复核一次，覆盖“调用方直接读 Messages() 通道（不经 Receive）”
	// 等没有读槽信号的场景，避免永久等待。
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		gs.mu.Lock()
		if gs.closed {
			gs.mu.Unlock()
			return
		}
		made := b.dispatchRoundLocked(gs)
		if made {
			gs.mu.Unlock()
			continue
		}
		w := gs.waitCh
		gs.mu.Unlock()
		select {
		case <-w: // channel-Cond：状态变化即唤醒，不丢信号
		case <-ticker.C: // 兜底复核
		}
	}
}

// dispatchRoundLocked 执行一轮派发（调用方持 gs.mu），返回是否派发出至少一条。
func (b *Bus) dispatchRoundLocked(gs *groupState) bool {
	if gs.closed || len(gs.onlineIDs) == 0 || len(gs.keyOrder) == 0 {
		return false
	}
	nKeys := len(gs.keyOrder)
	gs.keyRR %= nKeys
	start := gs.keyRR
	made := false
	for i := 0; i < nKeys; i++ {
		k := gs.keyOrder[(start+i)%nKeys]
		q := gs.perKey[k]
		if len(q) == 0 {
			continue
		}
		head := q[0]
		if head.leasedBy != "" {
			// 同 Key 在途未完成：后续消息必须等待（顺序保证的关键）。
			continue
		}
		ex := gs.freeExecLocked()
		if ex == nil {
			// 所有在线执行器在途窗口已满：本轮停止，等 Ack/Leave/Join。
			break
		}
		head.leasedBy = ex.id
		head.attempts++
		head.lastLeaseAt = time.Now()
		ex.inflight++
		d := &Delivery{Msg: envelopeToMessage(head.env), Attempt: head.attempts}

		// 非阻塞发送（空槽已由 inflight<queueN 预留）。通道关闭只可能
		// 发生在同样持 gs.mu 的 Close 路径，理论上不可见，recover 仅兜底，
		// 保证底层异常不冒到调用方。
		sent := true
		ch := ex.guard.get()
		func() {
			defer func() {
				if recover() != nil {
					sent = false
				}
			}()
			ch <- d
		}()
		if !sent {
			gs.releaseLeaseLocked(head, ex)
			continue
		}
		seq, execID := head.env.Seq, ex.id
		head.timer = time.AfterFunc(time.Duration(gs.cfg.ProcessTimeoutMs)*time.Millisecond,
			func() { b.onProcessTimeout(gs, seq, execID) })
		made = true
	}
	// 下一轮从下一个 Key 开始扫描，避免热点 Key 长期独占执行器。
	gs.keyRR = (start + 1) % nKeys
	return made
}

// onProcessTimeout 处理超时看门狗：消息仍由同一执行器持有时，
// 未超 MaxAttempts 则收回租约（交由其他在线执行器立即接手；无人
// 在线则保留待续），达到 MaxAttempts 则进死信队列（DeadProcessTimeout）。
func (b *Bus) onProcessTimeout(gs *groupState, seq uint64, execID string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if gs.closed {
		return
	}
	qm := gs.index[seq]
	if qm == nil || qm.leasedBy != execID {
		// 期间已被 Ack / 已被 Leave 释放 / 已转死信，本次看门狗作废。
		return
	}
	if qm.attempts >= gs.cfg.MaxAttempts {
		gs.moveDeadLocked(qm, DeadProcessTimeout, qm.attempts)
	} else {
		gs.releaseLeaseLocked(qm, gs.executors[execID])
	}
	gs.signalLocked()
}

// groupJanitor 周期扫描“排队等待接手过久”的队头消息（DeadWaitTimeout）。
// 只检查未带租约的队头：带租约的消息由处理超时看门狗负责，两类失败
// 原因彼此可区分。
func (b *Bus) groupJanitor(gs *groupState) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		gs.mu.Lock()
		if gs.closed {
			gs.mu.Unlock()
			return
		}
		wait := time.Duration(gs.cfg.WaitTimeoutMs) * time.Millisecond
		now := time.Now()
		keys := append([]string(nil), gs.keyOrder...)
		var timed []*queuedGroupMsg
		for _, k := range keys {
			q := gs.perKey[k]
			if len(q) == 0 {
				continue
			}
			head := q[0]
			if head.leasedBy == "" && now.Sub(head.enqueuedAt) >= wait {
				timed = append(timed, head)
			}
		}
		fired := false
		for _, qm := range timed {
			if _, ok := gs.index[qm.env.Seq]; !ok {
				continue
			}
			gs.moveDeadLocked(qm, DeadWaitTimeout, qm.attempts)
			fired = true
		}
		if fired {
			gs.signalLocked()
		}
		gs.mu.Unlock()
	}
}

// releaseLeaseLocked 收回消息租约：停看门狗、清空持有者、归还在途窗口。
// 消息仍保留在其 Key 队列（队头），可立即被其他在线执行器接手。
func (gs *groupState) releaseLeaseLocked(qm *queuedGroupMsg, ex *execState) {
	if qm.timer != nil {
		qm.timer.Stop()
		qm.timer = nil
	}
	qm.leasedBy = ""
	if ex != nil {
		ex.inflight--
	}
}

// moveDeadLocked 把当前保留在队列中的消息移入死信队列并释放其租约。
func (gs *groupState) moveDeadLocked(qm *queuedGroupMsg, reason DeadReason, attempts int) {
	if ex := gs.executors[qm.leasedBy]; ex != nil {
		gs.releaseLeaseLocked(qm, ex)
	}
	gs.removeFromKeyLocked(qm)
	qm.attempts = attempts
	gs.appendDeadLocked(qm, reason)
}

// addDeadLocked 把一条未入队的消息（如发布即溢出）直接写入死信队列。
func (gs *groupState) addDeadLocked(qm *queuedGroupMsg, reason DeadReason, attempts int) {
	qm.attempts = attempts
	gs.appendDeadLocked(qm, reason)
}

// appendDeadLocked 追加死信记录（保留消息本体供重新投递）。
func (gs *groupState) appendDeadLocked(qm *queuedGroupMsg, reason DeadReason) {
	gs.dead = append(gs.dead, &DeadLetter{
		Group:    gs.name,
		Topic:    gs.topic,
		Key:      qm.key,
		Reason:   reason,
		Attempts: qm.attempts,
		DeadAt:   time.Now(),
		Msg:      envelopeToMessage(qm.env),
		qm:       qm,
	})
}

// ---------- ExecutorGroup 对外方法 ----------

// Join 加入（或以 ResumeID 重新接入）分担组。
func (g *ExecutorGroup) Join(_ context.Context, opt ExecutorOption) (*Executor, error) {
	gs := g.st
	queueN := opt.QueueLen
	if queueN <= 0 {
		queueN = gs.cfg.ExecutorQueueLen
	}

	gs.mu.Lock()
	defer gs.mu.Unlock()
	if gs.closed {
		return nil, newError(KindClosed, gs.topic, "executor group already closed", nil)
	}

	var ex *execState
	if opt.ResumeID != "" {
		ex = gs.executors[opt.ResumeID]
		if ex == nil {
			return nil, newError(KindInvalidArgument, gs.topic,
				"cannot resume: executor id not found: "+opt.ResumeID, nil)
		}
		if ex.closed {
			return nil, newError(KindInvalidArgument, gs.topic,
				"cannot resume a closed executor: "+opt.ResumeID, nil)
		}
		if ex.online {
			return nil, newError(KindInvalidArgument, gs.topic,
				"executor already online: "+opt.ResumeID, nil)
		}
		// 重入：替换新通道，旧通道关闭丢弃其中尚未读走的副本；
		// 未 Ack 消息的租约此前已在 Leave 时释放，会按序重新派发。
		old := ex.guard.replace(make(chan *Delivery, queueN))
		ex.queueN = queueN
		ex.online = true
		ex.inflight = 0
		gs.onlineIDs = append(gs.onlineIDs, ex.id)
		safeCloseCh(old)
	} else {
		n := nextExecutorID(g.bus)
		ch := make(chan *Delivery, queueN)
		ex = &execState{
			id:     n,
			guard:  &chGuard{ch: ch},
			queueN: queueN,
			online: true,
		}
		gs.executors[n] = ex
		gs.onlineIDs = append(gs.onlineIDs, n)
	}
	gs.signalLocked()
	return &Executor{id: ex.id, topic: gs.topic, groupName: gs.name, st: gs, bus: g.bus}, nil
}

// DeadLetters 返回死信队列快照（不清空；顺序即进入死信的先后）。
func (g *ExecutorGroup) DeadLetters() []*DeadLetter {
	gs := g.st
	gs.mu.Lock()
	out := make([]*DeadLetter, len(gs.dead))
	copy(out, gs.dead)
	gs.mu.Unlock()
	return out
}

// RequeueDead 把指定 Seq 的死信按原 Key 重新投回分担组。
func (g *ExecutorGroup) RequeueDead(_ context.Context, seq uint64) error {
	gs := g.st
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if gs.closed {
		return newError(KindClosed, gs.topic, "executor group already closed", nil)
	}
	idx := -1
	for i, dl := range gs.dead {
		if dl.Msg != nil && dl.Msg.Seq == seq {
			idx = i
			break
		}
	}
	if idx < 0 {
		return newError(KindInvalidArgument, gs.topic,
			"message not in dead-letter queue", nil)
	}
	if gs.retainedLocked() >= gs.cfg.MaxBacklog {
		// 仍在积压上限：死信原样保留，调用方可稍后再试（有界、不静默）。
		return newError(KindOverflow, gs.topic,
			"backlog still full, dead letter retained", nil)
	}
	dl := gs.dead[idx]
	if _, exists := gs.index[seq]; exists {
		return newError(KindInvalidArgument, gs.topic,
			"message already retained", nil)
	}
	gs.dead = append(gs.dead[:idx], gs.dead[idx+1:]...)
	gs.requeueHeadLocked(dl.qm) // 插到原 Key 队首：顺序优先于同 Key 后续消息
	gs.signalLocked()
	return nil
}

// Stats 返回组运行态快照。
func (g *ExecutorGroup) Stats() GroupStats {
	gs := g.st
	gs.mu.Lock()
	s := gs.statsLocked()
	gs.mu.Unlock()
	return s
}

// Close 关闭分担组。
func (g *ExecutorGroup) Close() error {
	b := g.bus
	gs := g.st
	b.mu.Lock()
	if byTopic := b.groups[gs.topic]; byTopic != nil {
		delete(byTopic, gs.name)
	}
	b.mu.Unlock()

	gs.mu.Lock()
	if gs.closed {
		gs.mu.Unlock()
		return newError(KindClosed, gs.topic, "executor group already closed", nil)
	}
	gs.closed = true
	chs := make([]chan *Delivery, 0, len(gs.executors))
	for _, ex := range gs.executors {
		chs = append(chs, ex.guard.get())
		ex.closed = true
		ex.online = false
		for _, qm := range gs.index {
			if qm.leasedBy == ex.id && qm.timer != nil {
				qm.timer.Stop()
			}
		}
	}
	gs.onlineIDs = nil
	gs.signalLocked()
	gs.mu.Unlock()
	for _, ch := range chs {
		safeCloseCh(ch)
	}
	return nil
}

// ---------- Executor 对外方法 ----------

// Messages 返回执行器当前接收通道（ResumeID 重入后为重建的新通道）。
func (e *Executor) Messages() <-chan *Delivery {
	gs := e.st
	if gs == nil {
		return nil
	}
	gs.mu.Lock()
	var c <-chan *Delivery
	if ex := gs.executors[e.id]; ex != nil {
		c = ex.guard.get()
	}
	gs.mu.Unlock()
	return c
}

// Ack 确认完成：消息从组内保留集合永久移除，任何执行器都不会再收到。
// 重复 Ack、迟到 Ack（消息已转交他人）幂等。
func (e *Executor) Ack(seq uint64) error {
	gs := e.st
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if gs.closed {
		return newError(KindClosed, gs.topic, "executor group already closed", nil)
	}
	qm := gs.index[seq]
	if qm == nil {
		return nil // 已完成/已转死信：幂等
	}
	if qm.leasedBy != "" && qm.leasedBy != e.id {
		// 消息当前由别的执行器接手：忽略这条迟到确认，不影响新租约。
		return nil
	}
	holder := qm.leasedBy
	if qm.timer != nil {
		qm.timer.Stop()
		qm.timer = nil
	}
	gs.removeFromKeyLocked(qm)
	if holder != "" {
		if ex := gs.executors[holder]; ex != nil {
			ex.inflight--
		}
	}
	gs.signalLocked()
	return nil
}

// Receive 接收下一条消息并按主题 schema 解码（错误语义同 Subscription.Receive）。
func (e *Executor) Receive(ctx context.Context) (*Message, any, error) {
	if e.st == nil {
		return nil, nil, newError(KindInvalidArgument, e.topic, "nil executor", nil)
	}
	ch := e.Messages()
	var d *Delivery
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case dd, ok := <-ch:
		if !ok || dd == nil {
			return nil, nil, newError(KindClosed, e.topic, "executor channel closed", nil)
		}
		d = dd
	}
	// 读走即释放通道槽位（在途计数仍在，Ack 前仍受处理超时保护），
	// 派发协程可立即向该空槽继续派发。
	e.st.signalAfterRead()
	value, err := e.bus.decodeDelivery(d)
	if err != nil {
		return d.Msg, nil, err
	}
	return d.Msg, value, nil
}

// signalAfterRead 执行器读走一条消息后唤醒派发协程，使其尽快利用空槽。
// 在途计数仍计入（Ack 前受处理超时保护），因此不影响顺序与不重复语义。
func (gs *groupState) signalAfterRead() {
	gs.mu.Lock()
	gs.signalLocked()
	gs.mu.Unlock()
}

// Leave 执行器离开（掉线）：释放其全部在途租约，未 Ack 消息立即对其他
// 在线执行器开放；无其他在线执行器时保留在组内，等待其以 ResumeID
// 重新接入后续做。身份保留，消息不丢、不重复（已 Ack 的不在保留集）。
func (e *Executor) Leave(_ context.Context) error {
	gs := e.st
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if gs.closed {
		return newError(KindClosed, gs.topic, "executor group already closed", nil)
	}
	ex := gs.executors[e.id]
	if ex == nil || ex.closed {
		return newError(KindInvalidArgument, gs.topic, "executor not found or closed", nil)
	}
	if !ex.online {
		return nil
	}
	ex.online = false
	gs.removeOnlineLocked(e.id)
	for _, qm := range gs.index {
		if qm.leasedBy == e.id {
			gs.releaseLeaseLocked(qm, ex)
		}
	}
	gs.signalLocked()
	return nil
}

// Close 执行器永久退出：未 Ack 消息同样转交/保留，但身份不可再续。
func (e *Executor) Close() error {
	gs := e.st
	gs.mu.Lock()
	if gs.closed {
		gs.mu.Unlock()
		return newError(KindClosed, gs.topic, "executor group already closed", nil)
	}
	ex := gs.executors[e.id]
	if ex == nil {
		gs.mu.Unlock()
		return nil
	}
	ex.closed = true
	ex.online = false
	gs.removeOnlineLocked(e.id)
	for _, qm := range gs.index {
		if qm.leasedBy == e.id {
			gs.releaseLeaseLocked(qm, ex)
		}
	}
	delete(gs.executors, e.id)
	ch := ex.guard.get()
	gs.signalLocked()
	gs.mu.Unlock()
	safeCloseCh(ch)
	return nil
}
