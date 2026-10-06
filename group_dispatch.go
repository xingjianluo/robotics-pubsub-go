package pubsub

import (
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

// groupLoop 每个执行器组一个调度协程，把组内未确认消息按
// “键内队首（head-of-line）”原则投递给在线成员：
//
//   - 顺序：同一 Key 任一时刻只有队首一条在途，确认后才放行下一条，
//     因此同键消息严格按主题序号顺序，成员变更也不打乱；
//   - 独占：队首投递前把键绑定到某一在线成员（inFlight），
//     同一条消息不会同时派给两个成员；
//   - 故障转移：成员 Leave/Close 会释放其名下键（owner 清空、
//     inFlight 复位），调度器随即把未确认工作派给仍在线的成员；
//     没有在线成员时消息留在组内等待，不静默丢失；
//   - 失败语义：入组超过 DeliveryTimeoutMs 未确认 -> DeadTimeout；
//     投递尝试超过 MaxAttempts -> DeadMaxAttempts，均移入死信队列。
//
// 并发纪律与 subState 调度器相同：锁内决策、锁外阻塞发送；
// 发送可被成员离线（leaveCh 关闭）解除，不会因单个成员卡死全组，
// 也不持锁阻塞发布者。
func (b *Bus) groupLoop(g *groupState) {
	for {
		g.mu.Lock()
		if g.closed {
			g.mu.Unlock()
			return
		}
		// 1. 先把超时未确认的消息判死（状态变化后重新扫描）。
		if g.expireLocked(time.Now()) {
			g.mu.Unlock()
			continue
		}
		// 2. 选出下一条可投递的队首消息。
		ms, kq, gm, ok := b.pickDeliverableLocked(g)
		var wg *sync.WaitGroup
		if ok {
			// 在锁内登记在途发送（按当前代际的 WaitGroup）：Leave/Close
			// 关闭通道前会等待对应代际归零，而它们标记离线/换代际也在
			// 同一把锁下，因此登记与离线标记串行，
			// 不存在“已离线仍新增发送”的窗口。
			wg = ms.sendWg
			wg.Add(1)
		}
		if !ok {
			// 无可投递：等待信号或最近的超时点（避免忙轮询）。
			w := g.waitCh
			var timer *time.Timer
			var tc <-chan time.Time
			if dl, has := g.nextDeadlineLocked(time.Now()); has {
				timer = time.NewTimer(time.Until(dl))
				tc = timer.C
			}
			g.mu.Unlock()
			select {
			case <-w:
			case <-tc:
			}
			if timer != nil {
				timer.Stop()
			}
			continue
		}
		ch := ms.guard.get()
		leaveCh := ms.leaveCh
		seq := gm.env.Seq
		d := &Delivery{Msg: envelopeToMessage(gm.env), Attempt: gm.attempts}
		g.mu.Unlock()

		// 3. 锁外发送，三种出路：
		// 成功；成员离线（leaveCh 关闭）；成员在线但通道满（限时等待，
		// 避免一个慢成员永久堵死全组——超时的消息会由死信机制兜底）。
		sent := true
		func() {
			defer func() {
				if r := recover(); r != nil {
					sent = false
				}
			}()
			timer := time.NewTimer(groupSendWait)
			defer timer.Stop()
			select {
			case ch <- d:
			case <-leaveCh:
				sent = false
			case <-timer.C:
				sent = false
			}
		}()

		if sent {
			wg.Done()
			continue
		}
		// 发送未完成：不算一次有效投递，释放该键。属主仍在线则保留
		// 属主（下轮重试），已离线则清空属主以便转派其他成员。
		// 按“在途序号”核对：期间若有死信重投插到队首，这里仍能
		// 正确解除本序号的在途标志，不会误清也不会漏清。
		g.mu.Lock()
		if !g.closed {
			if cur := g.keys[gm.key]; cur == kq && kq.inFlight && kq.inFlightSeq == seq {
				kq.inFlight = false
				if m := g.members[kq.owner]; m == nil || !m.online || m.closed {
					kq.owner = ""
				}
				if gm.attempts > 0 {
					gm.attempts--
				}
			}
			g.signalLocked()
		}
		g.mu.Unlock()
		wg.Done()
	}
}

// groupSendWait 调度器对单个成员通道的阻塞发送上限。
const groupSendWait = 50 * time.Millisecond

// expireLocked 把超时未确认的消息移入死信队列，返回是否有状态变化。
func (g *groupState) expireLocked(now time.Time) bool {
	if g.timeoutMs <= 0 {
		return false
	}
	limit := time.Duration(g.timeoutMs) * time.Millisecond
	var expired []uint64
	for seq, gm := range g.pending {
		if now.Sub(gm.enqueuedAt) >= limit {
			expired = append(expired, seq)
		}
	}
	for _, seq := range expired {
		g.toDeadLocked(seq, DeadTimeout)
	}
	return len(expired) > 0
}

// nextDeadlineLocked 返回未确认消息中最近的超时点（用于定时唤醒）。
func (g *groupState) nextDeadlineLocked(now time.Time) (time.Time, bool) {
	if g.timeoutMs <= 0 {
		return time.Time{}, false
	}
	limit := time.Duration(g.timeoutMs) * time.Millisecond
	var best time.Time
	found := false
	for _, gm := range g.pending {
		dl := gm.enqueuedAt.Add(limit)
		if !found || dl.Before(best) {
			best = dl
			found = true
		}
	}
	return best, found
}

// pickDeliverableLocked 选出下一条可投递消息（调用方持锁）：
// 遍历键队列，找“队首未在途”的键；键没有在线属主时按 Key 哈希
// 绑定到某一在线成员；超过最大尝试次数的队首直接判死并重新扫描。
func (b *Bus) pickDeliverableLocked(g *groupState) (*memberState, *keyQueue, *groupMsg, bool) {
	for {
		restart := false
		for key, kq := range g.keys {
			if len(kq.seqs) == 0 || kq.inFlight {
				continue
			}
			head := kq.seqs[0]
			gm := g.pending[head]
			if gm == nil {
				continue
			}
			ms := g.members[kq.owner]
			if ms == nil || !ms.online || ms.closed {
				kq.owner = ""
				ms = pickMemberLocked(g, key)
				if ms == nil {
					continue // 无在线成员：留在组内等待，不丢不堵
				}
				kq.owner = ms.id
			}
			if g.maxAttempts > 0 && gm.attempts >= g.maxAttempts {
				g.toDeadLocked(head, DeadMaxAttempts)
				restart = true
				break
			}
			gm.attempts++
			kq.inFlight = true
			kq.inFlightSeq = head
			return ms, kq, gm, true
		}
		if !restart {
			return nil, nil, nil, false
		}
	}
}

// pickMemberLocked 按 Key 哈希在在线成员中确定性选主：
// 成员集合不变时同一 Key 总落到同一成员；成员变化只影响
// “待重新分配”的键，已被在线成员持有的键不迁移。
func pickMemberLocked(g *groupState, key string) *memberState {
	ids := make([]string, 0, len(g.members))
	for id, ms := range g.members {
		if ms.online && !ms.closed {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Strings(ids)
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return g.members[ids[int(h.Sum32())%len(ids)]]
}
