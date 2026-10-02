package pubsub

// Close 关闭总线：拒绝新发布/新订阅，唤醒并终止所有投递协程。
// 重复调用返回幂等的 KindClosed 错误。
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return newError(KindClosed, "", "bus already closed", nil)
	}
	b.closed = true
	states := make([]*subState, 0, len(b.byID))
	for _, st := range b.byID {
		states = append(states, st)
	}
	var groupStates []*groupState
	for _, byTopic := range b.groups {
		for _, st := range byTopic {
			groupStates = append(groupStates, st)
		}
	}
	b.mu.Unlock()

	// 分担组：标记关闭并关闭全部执行器通道（派发/清扫协程随后退出）。
	for _, gs := range groupStates {
		gs.mu.Lock()
		if gs.closed {
			gs.mu.Unlock()
			continue
		}
		gs.closed = true
		chs := make([]chan *Delivery, 0, len(gs.executors))
		for _, ex := range gs.executors {
			ex.closed = true
			ex.online = false
			chs = append(chs, ex.guard.get())
		}
		gs.onlineIDs = nil
		gs.signalLocked()
		gs.mu.Unlock()
		for _, ch := range chs {
			safeCloseCh(ch)
		}
	}

	for _, st := range states {
		st.mu.Lock()
		st.closed = true
		st.pending = map[uint64]*queuedMsg{}
		ch := st.guard.get()
		st.signalLocked()
		st.signalLocked()
		st.mu.Unlock()
		safeCloseCh(ch)
	}
	return nil
}
