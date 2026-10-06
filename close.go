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
	groups := make([]*groupState, 0)
	for _, byGroup := range b.groups {
		for _, g := range byGroup {
			groups = append(groups, g)
		}
	}
	b.mu.Unlock()

	for _, st := range states {
		st.mu.Lock()
		st.closed = true
		st.pending = map[uint64]*queuedMsg{}
		ch := st.guard.get()
		abort := st.abortCh
		st.abortCh = nil
		wg := st.sendWg
		st.signalLocked()
		st.signalLocked()
		st.mu.Unlock()
		// 先中止在途阻塞发送并等其归零，再关闭通道（消除 close/send 竞态）。
		if abort != nil {
			close(abort)
		}
		wg.Wait()
		safeCloseCh(ch)
	}
	// 关闭执行器组：调度协程退出，成员通道关闭，未确认状态随总线销毁。
	for _, g := range groups {
		g.mu.Lock()
		g.closed = true
		mss := make([]*memberState, 0, len(g.members))
		for _, ms := range g.members {
			ms.online = false
			ms.closed = true
			mss = append(mss, ms)
			safeCloseLeaveCh(ms.leaveCh)
		}
		g.signalLocked()
		g.mu.Unlock()
		for _, ms := range mss {
			// 等在途发送归零再关通道（避免 close/send 竞态）。
			ms.sendWg.Wait()
			safeCloseCh(ms.guard.get())
		}
	}
	return nil
}

func safeCloseLeaveCh(ch chan struct{}) {
	defer func() { _ = recover() }()
	close(ch)
}
