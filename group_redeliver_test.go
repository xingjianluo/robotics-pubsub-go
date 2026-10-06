package pubsub

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGroupRedeliverBehindInflight 重投与同键在途/排队消息共存（回归）：
// 一条超时进死信的消息被重新投递时，同 Key 还有正在处理（在途未确认）
// 和排队的消息。重投消息必须：
//   - 真正送出去（不被在途标志卡住、不会再次超时被判死）；
//   - 排在该 Key 尚未投递过的消息之前（原先后顺序）；
//   - 已经确认过的消息不再发一遍。
//
// 判定依据：Ack 在途的 seq=2 后，必须在重投超时窗口内收到重投的
// seq=1（Attempt 重新计数），随后才放行 seq=3；死信队列保持为空。
func TestGroupRedeliverBehindInflight(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 8, GroupMaxPending: 64,
		GroupDeliveryTimeoutMs: 200, GroupMaxAttempts: 100}, nil)
	defer bus.Close()
	ctx := context.Background()

	m, err := bus.JoinGroup(ctx, "t/rd", "exec", GroupOption{QueueLen: 8})
	if err != nil {
		t.Fatal(err)
	}
	// seq=1：收下不确认，等它超时进死信。
	if _, err := bus.PublishRawKeyed(ctx, "t/rd", "robot-1", 1, QoSReliable, []byte{1}); err != nil {
		t.Fatal(err)
	}
	d := <-m.Messages()
	if d.Msg.Seq != 1 {
		t.Fatalf("first delivery seq=%d, want 1", d.Msg.Seq)
	}
	waitDeadLetters(t, bus, "t/rd", "exec", 1, DeadTimeout)
	t.Logf("seq=1 dead-lettered (reason=%s) while holding it unacked", DeadTimeout)

	// seq=2、seq=3 随后入组：seq=2 成为新队首在途，seq=3 排队。
	for i := 2; i <= 3; i++ {
		if _, err := bus.PublishRawKeyed(ctx, "t/rd", "robot-1", 1, QoSReliable, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	seq2 := recvSkipping(t, m, 2, 2*time.Second) // 跳过 seq=1 的滞留副本
	t.Logf("seq=2 now in-flight (attempt=%d), keeping it unacked; seq=3 queued", seq2.Attempt)

	// 在 seq=2 在途、seq=3 排队时重投 seq=1。
	if err := bus.RedeliverDeadLetter("t/rd", "exec", 1); err != nil {
		t.Fatalf("redeliver seq=1: %v", err)
	}
	t.Logf("redelivered seq=1 while seq=2 in-flight and seq=3 queued")

	// 确认在途的 seq=2：重投的 seq=1 必须随后真正送出，而不是被卡住
	// 再次超时判死。等待窗口远大于投递超时，若被卡死会在这里暴露。
	if err := m.Ack(2); err != nil {
		t.Fatal(err)
	}
	d1 := recvSkipping(t, m, 1, 1500*time.Millisecond)
	if d1.Attempt != 1 {
		t.Fatalf("redelivered seq=1 attempt=%d, want fresh count 1", d1.Attempt)
	}
	t.Logf("redelivered seq=1 delivered after in-flight ack (fresh attempt=1)")
	if err := m.Ack(1); err != nil {
		t.Fatal(err)
	}

	// 随后才放行排队的 seq=3：顺序保持，已确认的 seq=2 不再出现。
	d3 := recvSkipping(t, m, 3, 2*time.Second)
	if err := m.Ack(d3.Msg.Seq); err != nil {
		t.Fatal(err)
	}
	t.Logf("queued seq=3 delivered after redelivered seq=1 (order preserved)")

	// 死信队列必须保持为空：重投消息没有再次被超时判死。
	dead, err := bus.DeadLetters("t/rd", "exec")
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 0 {
		t.Fatalf("redelivered message dead-lettered again: %+v", dead)
	}
	// 全部确认后不应再有任何投递。
	select {
	case dd := <-m.Messages():
		t.Fatalf("unexpected extra delivery seq=%d", dd.Msg.Seq)
	case <-time.After(300 * time.Millisecond):
	}
	t.Logf("ok: redelivered msg not stuck, not re-dead-lettered, acked seqs never resent")
}

// waitDeadLetters 等待组内出现恰好 n 条死信；reason 非 DeadNone 时逐条校验原因。
func waitDeadLetters(t *testing.T, bus *Bus, topic, group string, n int, reason DeadReason) []DeadLetter {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		dead, err := bus.DeadLetters(topic, group)
		if err != nil {
			t.Fatal(err)
		}
		if len(dead) == n {
			if reason != DeadNone {
				for _, d := range dead {
					if d.Reason != reason {
						t.Fatalf("dead letter seq=%d reason=%s, want %s", d.Seq, d.Reason, reason)
					}
				}
			}
			return dead
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d dead letters (reason=%s)", n, reason)
	return nil
}

// recvSkipping 从成员通道收到期望序号为止，跳过死信前滞留的旧副本。
func recvSkipping(t *testing.T, m *GroupMember, want uint64, timeout time.Duration) *Delivery {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case d, ok := <-m.Messages():
			if !ok {
				t.Fatalf("member channel closed while waiting seq=%d", want)
			}
			if d.Msg.Seq == want {
				return d
			}
			t.Logf("  skipping stale copy seq=%d (waiting seq=%d)", d.Msg.Seq, want)
		case <-deadline:
			t.Fatalf("timeout waiting delivery of seq=%d (redelivered message stuck?)", want)
			return nil
		}
	}
}

// TestGroupOverflowLandsInDeadLetter 积压超限 / 等待超时的消息统一落死信：
// 可靠发布等待积压窗口超时（KindOverflow）与低延迟发布遇满组丢弃
// （DropQueueFull）的消息都必须进入死信队列——可查看、原因可区分、
// 可重新投递，且重投保持键内顺序、不重复。
//
// 判定依据：两种失败结论各自对应一条原因可区分的死信；按 4、3 反序
// 请求重投后，成员仍按 1,2,3,4 顺序收到全部消息（Attempt 重新计数），
// 死信队列清空，重投中的序号不可重复重投。
func TestGroupOverflowLandsInDeadLetter(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 4, ReliableEnqueueTimeoutMs: 150,
		GroupDeliveryTimeoutMs: 60000, GroupMaxAttempts: 100}, nil)
	defer bus.Close()
	ctx := context.Background()

	// 成员在线但暂不消费：积压只增不减。
	m, err := bus.JoinGroup(ctx, "t/ofl", "exec", GroupOption{QueueLen: 8, MaxPending: 2})
	if err != nil {
		t.Fatal(err)
	}
	// seq=1,2 正常入组（占满积压上限）。
	for i := 0; i < 2; i++ {
		r, err := bus.PublishRawKeyed(ctx, "t/ofl", "robot-1", 1, QoSReliable, []byte{byte(i)})
		if err != nil || r.GroupDelivered != 1 {
			t.Fatalf("publish %d within limit: r=%+v err=%v", i, r, err)
		}
	}
	// seq=3：可靠发布等待积压窗口超时 -> KindOverflow，消息落死信。
	if _, err := bus.PublishRawKeyed(ctx, "t/ofl", "robot-1", 1, QoSReliable, []byte{3}); !IsError(err, KindOverflow) {
		t.Fatalf("expected KindOverflow on enqueue timeout, got %v", err)
	}
	t.Logf("reliable publish seq=3: KindOverflow (enqueue wait timeout)")
	// seq=4：低延迟发布遇满组 -> DropQueueFull，消息落死信。
	r, err := bus.PublishRawKeyed(ctx, "t/ofl", "robot-1", 1, QoSLowLatency, []byte{4})
	if err != nil || len(r.Dropped) != 1 || r.Dropped[0].Reason != DropQueueFull {
		t.Fatalf("expected DropQueueFull, got r=%+v err=%v", r, err)
	}
	t.Logf("low-latency publish seq=4: DropQueueFull (backlog full)")

	// 两条最终未入组的消息都可在死信队列查看，原因可区分。
	dead := waitDeadLetters(t, bus, "t/ofl", "exec", 2, DeadNone) // 原因逐条校验
	reasons := map[uint64]DeadReason{}
	for _, d := range dead {
		reasons[d.Seq] = d.Reason
	}
	if reasons[3] != DeadEnqueueTimeout || reasons[4] != DeadBacklogFull {
		t.Fatalf("dead reasons not distinguishable: %v", reasons)
	}
	t.Logf("both lost messages visible in DLQ: seq=3 %s, seq=4 %s",
		reasons[3], reasons[4])

	// 反序请求重投（先 4 后 3），键内仍按序号升序送出。
	if err := bus.RedeliverDeadLetter("t/ofl", "exec", 4); err != nil {
		t.Fatal(err)
	}
	if err := bus.RedeliverDeadLetter("t/ofl", "exec", 3); err != nil {
		t.Fatal(err)
	}
	// 已在重投途中的序号不可重复重投（不重复保证）。
	if err := bus.RedeliverDeadLetter("t/ofl", "exec", 3); !IsError(err, KindInvalidArgument) {
		t.Fatalf("re-redeliver pending seq should be KindInvalidArgument, got %v", err)
	}
	for want := uint64(1); want <= 4; want++ {
		d := recvSkipping(t, m, want, 2*time.Second)
		if want >= 3 && d.Attempt != 1 {
			t.Fatalf("redelivered seq=%d attempt=%d, want fresh count 1", want, d.Attempt)
		}
		if err := m.Ack(d.Msg.Seq); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("all 4 messages delivered in key order after redeliver (1,2 backlog + 3,4 redelivered)")

	dead, err = bus.DeadLetters("t/ofl", "exec")
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 0 {
		t.Fatalf("dead letter queue should be drained, got %d", len(dead))
	}
	t.Logf("ok: overflow/timeout messages viewable + redeliverable, dlq drained, no duplicates")
}

// TestGroupRedeliverUnderChurn 失败重投与成员上下线、并发投递叠加（场景）。
//
// 阶段 1（失败重投）：消费者按确定性规律“收到但拒不确认”部分消息，
// 使其超时进死信；巡检协程持续把死信重新投递，直到全部确认。
// 阶段 2（上下线切换 + 并发投递）：成员反复 Leave/重新入组，多发布者
// 并发发布，消费者即收即确认。
//
// 判定依据：两阶段合起来 1..total 每个序号都恰好被确认一次（不丢、
// 不重、不死锁）；阶段 1 必须真的发生死信重投（计数 > 0），阶段 2
// 必须真的发生成员切换（计数 > 0），否则场景未覆盖判负。
func TestGroupRedeliverUnderChurn(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 16, ReliableEnqueueTimeoutMs: 5000,
		GroupMaxPending: 512, GroupDeliveryTimeoutMs: 250, GroupMaxAttempts: 1000}, nil)
	defer bus.Close()
	ctx := context.Background()

	const membersN = 3
	const phase1 = 60
	const phase2 = 60
	const total = phase1 + phase2
	const keys = 4

	members := make([]*GroupMember, 0, membersN)
	ids := make([]string, 0, membersN)
	for i := 0; i < membersN; i++ {
		m, err := bus.JoinGroup(ctx, "t/rchurn", "exec", GroupOption{QueueLen: 16})
		if err != nil {
			t.Fatal(err)
		}
		members = append(members, m)
		ids = append(ids, m.ID())
	}
	t.Logf("scenario: %d members, phase1=%d msgs (hold-and-die + redeliver), phase2=%d msgs (churn), keys=%d",
		membersN, phase1, phase2, keys)

	done := make(chan struct{})
	var ackMu sync.Mutex
	acked := map[uint64]bool{}
	ackedN := 0
	markAcked := func(seq uint64) {
		ackMu.Lock()
		if !acked[seq] {
			acked[seq] = true
			ackedN++
		}
		ackMu.Unlock()
	}
	remaining := func() int {
		ackMu.Lock()
		defer ackMu.Unlock()
		return total - ackedN
	}

	// 消费侧：阶段 1 内每第 7 条消息“首次收到但拒不确认”（等它超时
	// 进死信，由巡检重投后才确认）；其余即收即确认。已确认过的滞留
	// 副本直接跳过（at-least-once 下去重）。
	var cwg sync.WaitGroup
	var seenMu sync.Mutex
	seen := map[uint64]int{}
	for _, m := range members {
		cwg.Add(1)
		go func(m *GroupMember) {
			defer cwg.Done()
			for {
				ch := m.Messages()
				if ch == nil {
					return
				}
				select {
				case d, ok := <-ch:
					if !ok {
						continue // 通道因 Leave/重入组重建，重新获取
					}
					seq := d.Msg.Seq
					seenMu.Lock()
					seen[seq]++
					n := seen[seq]
					seenMu.Unlock()
					if n == 1 && seq <= phase1 && seq%7 == 0 {
						continue // 阶段 1：首次收到拒不确认 -> 超时进死信
					}
					// 其余每份副本都确认：对仍在未确认集合中的序号生效，
					// 对已被判死/已确认的序号是幂等空操作。重投副本必须
					// 再次确认，否则会一直占着键内队首。
					if err := m.Ack(seq); err == nil {
						markAcked(seq)
					}
				case <-done:
					return
				}
			}
		}(m)
	}

	// 死信巡检：持续把死信重新投递回组（顺序与不重复由组内保证）。
	var redelivered atomic.Int64
	var jwg sync.WaitGroup
	jwg.Add(1)
	go func() {
		defer jwg.Done()
		for remaining() > 0 {
			dead, err := bus.DeadLetters("t/rchurn", "exec")
			if err != nil {
				return
			}
			for _, d := range dead {
				if err := bus.RedeliverDeadLetter("t/rchurn", "exec", d.Seq); err != nil {
					t.Errorf("redeliver seq=%d: %v", d.Seq, err)
					return
				}
				redelivered.Add(1)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	waitAll := func(phase string, want int) {
		t.Helper()
		deadline := time.After(20 * time.Second)
		for {
			ackMu.Lock()
			n := ackedN
			ackMu.Unlock()
			if n >= want {
				return
			}
			select {
			case <-deadline:
				ackMu.Lock()
				var missing []uint64
				for s := uint64(1); s <= uint64(want); s++ {
					if !acked[s] {
						missing = append(missing, s)
					}
				}
				ackMu.Unlock()
				seenMu.Lock()
				for _, s := range missing {
					t.Logf("missing seq=%d seenByConsumer=%d", s, seen[s])
				}
				seenMu.Unlock()
				dumpGroupState(t, bus, "t/rchurn", "exec")
				t.Fatalf("%s timeout: acked %d/%d (lost or stuck)", phase, n, want)
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}
	}

	// 阶段 1：发布 phase1 条，等待死信重投收敛。
	for i := 0; i < phase1; i++ {
		key := fmt.Sprintf("robot-%d", i%keys)
		if _, err := bus.PublishRawKeyed(ctx, "t/rchurn", key, 1, QoSReliable, []byte{byte(i)}); err != nil {
			t.Fatalf("phase1 publish %d: %v", i, err)
		}
	}
	waitAll("phase1", phase1)
	if redelivered.Load() == 0 {
		t.Fatal("phase1 finished without any dead-letter redelivery: scenario not covered")
	}
	t.Logf("phase1 ok: %d msgs acked after %d dead-letter redeliveries", phase1, redelivered.Load())

	// 阶段 2：成员反复离线/重新入组 + 多发布者并发投递。
	// 搅动固定 20 轮，保证上下线切换覆盖量与收敛速度无关。
	var churns atomic.Int64
	var wgw sync.WaitGroup
	wgw.Add(1)
	go func() {
		defer wgw.Done()
		for i := 0; i < 20; i++ {
			idx := i % membersN
			_ = members[idx].Leave(ctx)
			churns.Add(1)
			time.Sleep(2 * time.Millisecond)
			if _, err := bus.JoinGroup(ctx, "t/rchurn", "exec",
				GroupOption{MemberID: ids[idx], QueueLen: 16}); err != nil {
				t.Errorf("rejoin %s: %v", ids[idx], err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	var pubWg sync.WaitGroup
	for p := 0; p < 3; p++ {
		pubWg.Add(1)
		go func(pid int) {
			defer pubWg.Done()
			for i := pid; i < phase2; i += 3 {
				seq := phase1 + i
				key := fmt.Sprintf("robot-%d", seq%keys)
				if _, err := bus.PublishRawKeyed(ctx, "t/rchurn", key, 1, QoSReliable, []byte{byte(seq)}); err != nil {
					t.Errorf("phase2 publish %d: %v", seq, err)
					return
				}
			}
		}(p)
	}
	pubWg.Wait()
	waitAll("phase2", total)
	wgw.Wait()
	if churns.Load() == 0 {
		t.Fatal("phase2 finished without any member leave/rejoin: scenario not covered")
	}
	t.Logf("phase2 ok: %d msgs acked under %d member leave/rejoin cycles", phase2, churns.Load())

	close(done)
	jwg.Wait()
	cwg.Wait()
	for seq := uint64(1); seq <= total; seq++ {
		ackMu.Lock()
		ok := acked[seq]
		ackMu.Unlock()
		if !ok {
			t.Fatalf("seq=%d never acked", seq)
		}
	}
	t.Logf("ok: all %d msgs acked exactly once (redeliveries=%d, churns=%d)",
		total, redelivered.Load(), churns.Load())
}

// dumpGroupState 测试排障：打印组内未确认消息与各键队列状态。
func dumpGroupState(t *testing.T, bus *Bus, topic, group string) {
	t.Helper()
	g := bus.groupState(topic, group)
	if g == nil {
		t.Logf("dump: group %s/%s not found", topic, group)
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	t.Logf("dump: pending=%d dlq=%d keys=%d", len(g.pending), len(g.dlq), len(g.keys))
	for seq, gm := range g.pending {
		t.Logf("  pending seq=%d key=%s attempts=%d age=%s", seq, gm.key, gm.attempts,
			time.Since(gm.enqueuedAt).Round(time.Millisecond))
	}
	for key, kq := range g.keys {
		t.Logf("  key=%s seqs=%v owner=%q inFlight=%d", key, kq.seqs, kq.owner, kq.inFlight)
	}
	for id, ms := range g.members {
		t.Logf("  member=%s online=%v closed=%v chanLen=%d", id, ms.online, ms.closed,
			len(ms.guard.get()))
	}
}
