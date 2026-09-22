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
	b.mu.Unlock()

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
