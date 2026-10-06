package pubsub

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// groupRecorder 并发安全地记录组内各成员收到的投递。
type groupRecorder struct {
	mu       sync.Mutex
	byMember map[string][]uint64 // memberID -> 收到的序号（按收到顺序）
	byKey    map[string][]uint64 // key -> 收到的序号（按收到顺序）
	seqs     map[uint64]int      // seq -> 被投递次数
}

func newGroupRecorder() *groupRecorder {
	return &groupRecorder{
		byMember: map[string][]uint64{},
		byKey:    map[string][]uint64{},
		seqs:     map[uint64]int{},
	}
}

func (r *groupRecorder) add(memberID, key string, seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byMember[memberID] = append(r.byMember[memberID], seq)
	r.byKey[key] = append(r.byKey[key], seq)
	r.seqs[seq]++
}

func (r *groupRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seqs)
}

// TestGroupSharedConsumption 同一主题挂多个执行器共同分担：
// 每条可靠消息只被一个成员接手，全部消息恰好各投递一次，
// 同一 Key 的消息严格按序号顺序到达。
func TestGroupSharedConsumption(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 16, GroupMaxPending: 512,
		GroupDeliveryTimeoutMs: 60000, GroupMaxAttempts: 100}, nil)
	defer bus.Close()
	ctx := context.Background()

	const members = 3
	ms := make([]*GroupMember, 0, members)
	for i := 0; i < members; i++ {
		m, err := bus.JoinGroup(ctx, "t/grp", "exec", GroupOption{QueueLen: 16})
		if err != nil {
			t.Fatal(err)
		}
		ms = append(ms, m)
	}
	t.Logf("members joined: %s %s %s", ms[0].ID(), ms[1].ID(), ms[2].ID())

	const total = 60
	const keys = 6
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("robot-%d", i%keys)
		r, err := bus.PublishRawKeyed(ctx, "t/grp", key, 1, QoSReliable, []byte{byte(i)})
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if r.GroupDelivered != 1 {
			t.Fatalf("seq=%d GroupDelivered=%d, want 1", r.Seq, r.GroupDelivered)
		}
	}

	rec := newGroupRecorder()
	done := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	for _, m := range ms {
		wg.Add(1)
		go func(m *GroupMember) {
			defer wg.Done()
			for {
				select {
				case d, ok := <-m.Messages():
					if !ok {
						return
					}
					rec.add(m.ID(), d.Msg.Key, d.Msg.Seq)
					if err := m.Ack(d.Msg.Seq); err != nil {
						t.Errorf("ack: %v", err)
					}
					if rec.count() == total {
						once.Do(func() { close(done) })
					}
				case <-done:
					return
				}
			}
		}(m)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout: received %d/%d", rec.count(), total)
	}
	wg.Wait()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	// 判定 1：每条消息恰好被一个成员接手一次（无重复、无丢失）。
	for seq := uint64(1); seq <= total; seq++ {
		if rec.seqs[seq] != 1 {
			t.Fatalf("seq=%d delivered %d times, want exactly 1", seq, rec.seqs[seq])
		}
	}
	// 判定 2：同一 Key（机器人）的消息严格按序号升序。
	for key, seqs := range rec.byKey {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("key %s order broken: %v", key, seqs)
			}
		}
	}
	// 判定 3：负载确实被多个成员分担（6 个键哈希到 3 个成员）。
	if len(rec.byMember) < 2 {
		t.Fatalf("load not shared: only members %v received", rec.byMember)
	}
	t.Logf("shared consumption: %d msgs across %d members, per-key order ok, exactly-once handoff",
		total, len(rec.byMember))
	for id, seqs := range rec.byMember {
		t.Logf("  member %s handled %d msgs", id, len(seqs))
	}
}

// TestGroupMemberFailover 执行器中途掉线：已确认的不重投，
// 未确认的工作转给在线成员且保持键内顺序。
func TestGroupMemberFailover(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 16, GroupMaxPending: 64,
		GroupDeliveryTimeoutMs: 60000, GroupMaxAttempts: 100}, nil)
	defer bus.Close()
	ctx := context.Background()

	m1, err := bus.JoinGroup(ctx, "t/fo", "exec", GroupOption{QueueLen: 16})
	if err != nil {
		t.Fatal(err)
	}
	// 同一机器人 Key 的 3 条消息。
	for i := 0; i < 3; i++ {
		if _, err := bus.PublishRawKeyed(ctx, "t/fo", "robot-1", 1, QoSReliable, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}

	// m1 收下 seq=1 并确认；收下 seq=2 但不确认，随后掉线。
	d1 := <-m1.Messages()
	if d1.Msg.Seq != 1 {
		t.Fatalf("m1 first delivery seq=%d, want 1", d1.Msg.Seq)
	}
	if err := m1.Ack(1); err != nil {
		t.Fatal(err)
	}
	d2 := <-m1.Messages()
	if d2.Msg.Seq != 2 {
		t.Fatalf("m1 second delivery seq=%d, want 2", d2.Msg.Seq)
	}
	t.Logf("m1 acked seq=1, holds seq=2 unacked, now leaving")
	if err := m1.Leave(ctx); err != nil {
		t.Fatal(err)
	}

	// m2 上线：应接手 seq=2（重投，Attempt>=2），随后 seq=3，顺序不乱；
	// 已确认的 seq=1 绝不再派。
	m2, err := bus.JoinGroup(ctx, "t/fo", "exec", GroupOption{QueueLen: 16})
	if err != nil {
		t.Fatal(err)
	}
	// 键内队首语义：确认当前队首后才放行下一条。
	recv := func(want uint64) *Delivery {
		t.Helper()
		select {
		case d := <-m2.Messages():
			if d.Msg.Seq == 1 {
				t.Fatalf("acked seq=1 redelivered to m2")
			}
			if d.Msg.Seq != want {
				t.Fatalf("m2 got seq=%d, want %d (failover order broken)", d.Msg.Seq, want)
			}
			return d
		case <-time.After(3 * time.Second):
			t.Fatalf("m2 did not take over unacked work (waiting seq=%d)", want)
			return nil
		}
	}
	g2 := recv(2)
	if g2.Attempt < 2 {
		t.Fatalf("transferred seq=2 should be a redelivery, attempt=%d", g2.Attempt)
	}
	if err := m2.Ack(2); err != nil {
		t.Fatal(err)
	}
	g3 := recv(3)
	if err := m2.Ack(3); err != nil {
		t.Fatal(err)
	}
	t.Logf("failover ok: m2 took over seq=2 (attempt=%d) then seq=3 in order; acked seq=1 never resent",
		g2.Attempt)
	_ = g3
}

// TestGroupRejoinResume 执行器处理到一半离开、稍后回来：
// 离线期间的消息在组内积压不丢，重新入组后按序续传，已确认不重复。
func TestGroupRejoinResume(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 16, GroupMaxPending: 64,
		GroupDeliveryTimeoutMs: 60000, GroupMaxAttempts: 100}, nil)
	defer bus.Close()
	ctx := context.Background()

	m1, err := bus.JoinGroup(ctx, "t/rejoin", "exec", GroupOption{QueueLen: 16})
	if err != nil {
		t.Fatal(err)
	}
	id := m1.ID()
	for i := 0; i < 2; i++ {
		if _, err := bus.PublishRawKeyed(ctx, "t/rejoin", "robot-1", 1, QoSReliable, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	d := <-m1.Messages()
	if err := m1.Ack(d.Msg.Seq); err != nil { // 确认 seq=1
		t.Fatal(err)
	}
	d2 := <-m1.Messages() // seq=2 留在手里不确认
	if d2.Msg.Seq != 2 {
		t.Fatalf("seq=%d, want 2", d2.Msg.Seq)
	}
	if err := m1.Leave(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("member %s left with seq=2 unacked; publishing 3 more while all offline", id)

	// 全部成员离线：发布仍然成功，消息在组内积压（不丢、不堵发布者）。
	for i := 0; i < 3; i++ {
		r, err := bus.PublishRawKeyed(ctx, "t/rejoin", "robot-1", 1, QoSReliable, []byte{10 + byte(i)})
		if err != nil {
			t.Fatalf("publish while all offline must not fail: %v", err)
		}
		if r.GroupDelivered != 1 {
			t.Fatalf("offline backlog not retained: %+v", r)
		}
	}

	// 原成员回来：应从最小未确认 seq=2 开始按序续传，seq=1 不重复。
	m1b, err := bus.JoinGroup(ctx, "t/rejoin", "exec", GroupOption{MemberID: id, QueueLen: 16})
	if err != nil {
		t.Fatal(err)
	}
	if m1b.ID() != id {
		t.Fatalf("rejoin id mismatch %s != %s", m1b.ID(), id)
	}
	for want := uint64(2); want <= 5; want++ {
		select {
		case dd := <-m1b.Messages():
			if dd.Msg.Seq != want {
				t.Fatalf("resumed delivery seq=%d, want %d (order/no-dup violation)", dd.Msg.Seq, want)
			}
			_ = m1b.Ack(dd.Msg.Seq)
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout waiting resumed seq=%d", want)
		}
	}
	t.Logf("rejoin resume ok: member %s finished seq 2..5 in order, acked seq=1 not repeated", id)
}

// TestGroupBacklogOverflow 组积压超限：可靠发布得到可区分的
// KindOverflow（不静默丢失、已入队保留），且不拖住其他主题的发布者。
func TestGroupBacklogOverflow(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 4, ReliableEnqueueTimeoutMs: 150,
		GroupDeliveryTimeoutMs: 60000, GroupMaxAttempts: 100}, nil)
	defer bus.Close()
	ctx := context.Background()

	// 成员在线但从不消费：积压只增不减。
	if _, err := bus.JoinGroup(ctx, "t/full", "exec",
		GroupOption{QueueLen: 4, MaxPending: 4}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := bus.PublishRawKeyed(ctx, "t/full", "robot-1", 1, QoSReliable, []byte{byte(i)}); err != nil {
			t.Fatalf("publish %d within backlog limit: %v", i, err)
		}
	}
	// 第 5 条：组积压超限，有界等待后返回 KindOverflow。
	_, err := bus.PublishRawKeyed(ctx, "t/full", "robot-1", 1, QoSReliable, []byte{4})
	if err == nil || !IsError(err, KindOverflow) {
		t.Fatalf("expected KindOverflow on backlog overflow, got %v", err)
	}
	t.Logf("backlog overflow conclusion: %v", err)

	// 其他主题的发布者不受拖累。
	doneCh := make(chan error, 1)
	go func() {
		_, e := bus.PublishRaw(ctx, "t/other", 1, QoSLowLatency, []byte{9})
		doneCh <- e
	}()
	select {
	case e := <-doneCh:
		if e != nil {
			t.Fatalf("unrelated topic publish failed: %v", e)
		}
		t.Logf("unrelated topic publish unaffected (no cross-topic blocking)")
	case <-time.After(time.Second):
		t.Fatal("unrelated publisher blocked by full group")
	}

	// 低延迟发布到满组：不阻塞，给出可区分丢弃原因。
	r, err := bus.PublishRawKeyed(ctx, "t/full", "robot-1", 1, QoSLowLatency, []byte{5})
	if err != nil {
		t.Fatalf("low-latency publish must not error: %v", err)
	}
	if len(r.Dropped) != 1 || r.Dropped[0].Reason != DropQueueFull {
		t.Fatalf("expected DropQueueFull for full group, got %+v", r.Dropped)
	}
	t.Logf("low-latency publish to full group: dropped with reason %s", r.Dropped[0].Reason)
}

// TestGroupBacklogOverflowDeadLetter 积压超限/等待入队超时被拒收的消息
// （可靠发布 KindOverflow、低延迟发布 DropQueueFull）必须统一落入组
// 死信队列：可查看、原因可区分、可重新投递，且重投保序不重复。
func TestGroupBacklogOverflowDeadLetter(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 4, ReliableEnqueueTimeoutMs: 150,
		GroupDeliveryTimeoutMs: 60000, GroupMaxAttempts: 100}, nil)
	defer bus.Close()
	ctx := context.Background()

	// 成员在线但先不消费：积压只增不减。
	m, err := bus.JoinGroup(ctx, "t/ovfl", "exec",
		GroupOption{QueueLen: 4, MaxPending: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := bus.PublishRawKeyed(ctx, "t/ovfl", "robot-1", 1, QoSReliable, []byte{byte(i)}); err != nil {
			t.Fatalf("publish %d within backlog limit: %v", i, err)
		}
	}
	// 场景 1：可靠发布等待入组窗口超时 -> KindOverflow，且消息落死信。
	_, err = bus.PublishRawKeyed(ctx, "t/ovfl", "robot-1", 1, QoSReliable, []byte{2})
	if err == nil || !IsError(err, KindOverflow) {
		t.Fatalf("expected KindOverflow on backlog overflow, got %v", err)
	}
	// 场景 2：低延迟发布遇满组 -> DropQueueFull，消息同样落死信。
	r, err := bus.PublishRawKeyed(ctx, "t/ovfl", "robot-1", 1, QoSLowLatency, []byte{3})
	if err != nil {
		t.Fatalf("low-latency publish must not error: %v", err)
	}
	if len(r.Dropped) != 1 || r.Dropped[0].Reason != DropQueueFull {
		t.Fatalf("expected DropQueueFull for full group, got %+v", r.Dropped)
	}

	// 判定 1：两条被拒收的消息都能在死信队列看到，原因与处理超时/尝试
	// 超限可区分（DeadBacklogOverflow），序号与负载完整。
	dead, err := bus.DeadLetters("t/ovfl", "exec")
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 2 {
		t.Fatalf("expected 2 overflow dead letters, got %+v", dead)
	}
	for i, d := range dead {
		wantSeq := uint64(3 + i)
		if d.Seq != wantSeq || d.Reason != DeadBacklogOverflow {
			t.Fatalf("dead letter %+v, want seq=%d reason=%s", d, wantSeq, DeadBacklogOverflow)
		}
		if len(d.Payload) != 1 || d.Payload[0] != byte(2+i) {
			t.Fatalf("dead letter payload lost: %+v", d)
		}
	}
	t.Logf("overflow conclusions kept (KindOverflow / DropQueueFull); "+
		"both rejected messages landed in dlq: seq=%d,%d reason=%s",
		dead[0].Seq, dead[1].Seq, dead[0].Reason)

	// 排空：成员开始消费，先收齐组内积压的 seq=1,2。
	recv := func(want uint64) {
		t.Helper()
		select {
		case d := <-m.Messages():
			if d.Msg.Seq != want {
				t.Fatalf("delivered seq=%d, want %d (order violation)", d.Msg.Seq, want)
			}
			if err := m.Ack(d.Msg.Seq); err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting seq=%d", want)
		}
	}
	recv(1)
	recv(2)

	// 判定 2：两条死信可按原序号重投回组，按序送达、attempts 重新计数。
	if err := bus.RedeliverDeadLetter("t/ovfl", "exec", 4); err != nil {
		t.Fatal(err)
	}
	if err := bus.RedeliverDeadLetter("t/ovfl", "exec", 3); err != nil {
		t.Fatal(err)
	}
	recv(3) // 即使按 4、3 顺序请求重投，键内仍按序号升序送达
	recv(4)
	dead, _ = bus.DeadLetters("t/ovfl", "exec")
	if len(dead) != 0 {
		t.Fatalf("dead letter queue should be drained after redelivery, got %+v", dead)
	}
	select {
	case d := <-m.Messages():
		t.Fatalf("unexpected extra delivery: seq=%d", d.Msg.Seq)
	case <-time.After(300 * time.Millisecond):
	}
	t.Logf("overflow dead letters redelivered in order (seq 3,4), dlq drained, no duplicates")
}

// TestGroupDeadLetterTimeoutAndRedeliver 迟迟不被处理的消息进入
// 死信队列（原因可区分），可查看、可重新投递，且重投保持顺序不重复。
func TestGroupDeadLetterTimeoutAndRedeliver(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 8, GroupMaxPending: 64,
		GroupDeliveryTimeoutMs: 200, GroupMaxAttempts: 100}, nil)
	defer bus.Close()
	ctx := context.Background()

	m, err := bus.JoinGroup(ctx, "t/dlq", "exec", GroupOption{QueueLen: 8})
	if err != nil {
		t.Fatal(err)
	}
	// 同一 Key 两条消息；成员收下但永不确认。
	for i := 0; i < 2; i++ {
		if _, err := bus.PublishRawKeyed(ctx, "t/dlq", "robot-1", 1, QoSReliable, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// 等超时判死。
	var dead []DeadLetter
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		dead, err = bus.DeadLetters("t/dlq", "exec")
		if err != nil {
			t.Fatal(err)
		}
		if len(dead) == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(dead) != 2 {
		t.Fatalf("expected 2 dead letters, got %d", len(dead))
	}
	for _, d := range dead {
		if d.Reason != DeadTimeout {
			t.Fatalf("dead letter seq=%d reason=%s, want %s", d.Seq, d.Reason, DeadTimeout)
		}
	}
	t.Logf("dead letters: seq=%d,%d reason=%s (distinguishable)", dead[0].Seq, dead[1].Seq, dead[0].Reason)

	// 非死信序号重投应得到可区分错误。
	if err := bus.RedeliverDeadLetter("t/dlq", "exec", 99); !IsError(err, KindInvalidArgument) {
		t.Fatalf("redeliver non-dead seq should be KindInvalidArgument, got %v", err)
	}

	// 排空成员通道里滞留的旧副本（判死前已送入通道的部分）。
	for {
		select {
		case <-m.Messages():
		default:
			goto drained
		}
	}
drained:

	// 重新投递：按原序号顺序回到组内，attempts 重新计数。
	if err := bus.RedeliverDeadLetter("t/dlq", "exec", 2); err != nil {
		t.Fatal(err)
	}
	if err := bus.RedeliverDeadLetter("t/dlq", "exec", 1); err != nil {
		t.Fatal(err)
	}
	// 即使按 2、1 顺序请求重投，键内仍按序号升序投递。
	for want := uint64(1); want <= 2; want++ {
		select {
		case d := <-m.Messages():
			if d.Msg.Seq != want {
				t.Fatalf("redelivered seq=%d, want %d (order not preserved)", d.Msg.Seq, want)
			}
			if d.Attempt != 1 {
				t.Fatalf("redelivered seq=%d attempt=%d, want fresh count 1", d.Msg.Seq, d.Attempt)
			}
			_ = m.Ack(d.Msg.Seq)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting redelivered seq=%d", want)
		}
	}
	// 死信队列已清空，确认过的消息不会再来。
	dead, _ = bus.DeadLetters("t/dlq", "exec")
	if len(dead) != 0 {
		t.Fatalf("dead letter queue should be drained, got %d", len(dead))
	}
	select {
	case d := <-m.Messages():
		t.Fatalf("unexpected extra delivery after ack: seq=%d", d.Msg.Seq)
	case <-time.After(300 * time.Millisecond):
	}
	t.Logf("redeliver ok: seq 1,2 redelivered in order with fresh attempts, dlq drained, no duplicates")
}

// TestGroupRedeliverWhileKeyBusy 回归：死信重投时，同一个 Key 还有
// 正在处理（在途）或排队的消息——重投消息必须能真正送出、按原顺序
// 插回队首、不重复、不被卡死（此前在途标志按“位置”清理，重投插到
// 队首后标志永远清不掉，重投消息会被卡住直到再次超时被判死）。
func TestGroupRedeliverWhileKeyBusy(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 8, GroupMaxPending: 64,
		GroupDeliveryTimeoutMs: 60000, GroupMaxAttempts: 1}, nil)
	defer bus.Close()
	ctx := context.Background()

	m1, err := bus.JoinGroup(ctx, "t/redl", "exec", GroupOption{QueueLen: 8})
	if err != nil {
		t.Fatal(err)
	}
	id := m1.ID()
	recv := func(want uint64, timeout time.Duration) *Delivery {
		t.Helper()
		select {
		case d := <-m1.Messages():
			if d.Msg.Seq != want {
				t.Fatalf("delivered seq=%d, want %d (order/no-dup violation)", d.Msg.Seq, want)
			}
			return d
		case <-time.After(timeout):
			t.Fatalf("timeout waiting seq=%d (redelivery stuck behind busy key)", want)
			return nil
		}
	}

	// 场景 1：构造死信。MaxAttempts=1，seq=2 投递后不确认就掉线，
	// 重新入组时尝试次数已达上限 -> 判死（原因 DeadMaxAttempts）。
	for i := 0; i < 3; i++ {
		if _, err := bus.PublishRawKeyed(ctx, "t/redl", "robot-1", 1, QoSReliable, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	d := recv(1, 2*time.Second)
	if err := m1.Ack(d.Msg.Seq); err != nil {
		t.Fatal(err)
	}
	d = recv(2, 2*time.Second) // seq=2 留在手里不确认
	if err := m1.Leave(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("step1: seq=1 acked, seq=2 delivered(attempt=%d) then member left unacked", d.Attempt)
	if _, err := bus.JoinGroup(ctx, "t/redl", "exec", GroupOption{MemberID: id, QueueLen: 8}); err != nil {
		t.Fatal(err)
	}
	// 判定 1：seq=2 因尝试超限进入死信（seq=3 能被投递即证明 2 已出队判死）。
	d = recv(3, 2*time.Second)
	dead, err := bus.DeadLetters("t/redl", "exec")
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].Seq != 2 || dead[0].Reason != DeadMaxAttempts {
		t.Fatalf("expected seq=2 dead with %s, got %+v", DeadMaxAttempts, dead)
	}
	t.Logf("step2: seq=2 dead-lettered (reason=%s), seq=3 now in flight (held unacked)", dead[0].Reason)

	// 场景 2：同键有在途（seq=3）+ 排队（seq=4）消息时重投死信 seq=2。
	if err := bus.RedeliverDeadLetter("t/redl", "exec", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.PublishRawKeyed(ctx, "t/redl", "robot-1", 1, QoSReliable, []byte{9}); err != nil {
		t.Fatal(err)
	}
	t.Logf("step3: redelivered dead seq=2 while seq=3 in-flight and seq=4 queued")

	// 判定 2：确认在途的 seq=3 后，重投的 seq=2 必须真正送出（不被卡死），
	// 且排在新消息 seq=4 之前（键内原顺序），attempts 重新计数。
	if err := m1.Ack(3); err != nil {
		t.Fatal(err)
	}
	d = recv(2, 2*time.Second)
	if d.Attempt != 1 {
		t.Fatalf("redelivered seq=2 attempt=%d, want fresh count 1", d.Attempt)
	}
	if err := m1.Ack(2); err != nil {
		t.Fatal(err)
	}
	t.Logf("step4: redelivered seq=2 delivered after in-flight seq=3 acked, attempt reset to 1")
	d = recv(4, 2*time.Second)
	if err := m1.Ack(4); err != nil {
		t.Fatal(err)
	}

	// 判定 3：全部确认后不再有重复投递，死信队列已清空。
	select {
	case dd := <-m1.Messages():
		t.Fatalf("unexpected extra delivery: seq=%d", dd.Msg.Seq)
	case <-time.After(300 * time.Millisecond):
	}
	dead, _ = bus.DeadLetters("t/redl", "exec")
	if len(dead) != 0 {
		t.Fatalf("dead letter queue should be drained, got %+v", dead)
	}
	t.Logf("redeliver-while-busy ok: seq 2 went out in key order, no stuck, no duplicates, dlq drained")
}


// TestGroupMaxAttempts 投递尝试超限的消息判死进死信队列，
// 原因与超时判死可区分。
func TestGroupMaxAttempts(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 8, GroupMaxPending: 64,
		GroupDeliveryTimeoutMs: 60000, GroupMaxAttempts: 2}, nil)
	defer bus.Close()
	ctx := context.Background()

	m, err := bus.JoinGroup(ctx, "t/att", "exec", GroupOption{QueueLen: 8, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	id := m.ID()
	if _, err := bus.PublishRawKeyed(ctx, "t/att", "robot-1", 1, QoSReliable, []byte{1}); err != nil {
		t.Fatal(err)
	}
	// 两轮“收到但不确认就掉线”：尝试次数耗到上限。
	for round := 1; round <= 2; round++ {
		select {
		case d := <-m.Messages():
			if d.Attempt != round {
				t.Fatalf("round %d: attempt=%d", round, d.Attempt)
			}
			t.Logf("round %d: delivered seq=%d attempt=%d, leaving without ack", round, d.Msg.Seq, d.Attempt)
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: no delivery", round)
		}
		if err := m.Leave(ctx); err != nil {
			t.Fatal(err)
		}
		if round < 2 {
			if _, err := bus.JoinGroup(ctx, "t/att", "exec", GroupOption{MemberID: id}); err != nil {
				t.Fatal(err)
			}
		}
	}
	// 重新入组触发调度：attempts 已达上限，应判死而不是再投。
	if _, err := bus.JoinGroup(ctx, "t/att", "exec", GroupOption{MemberID: id}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		dead, err := bus.DeadLetters("t/att", "exec")
		if err != nil {
			t.Fatal(err)
		}
		if len(dead) == 1 {
			if dead[0].Reason != DeadMaxAttempts {
				t.Fatalf("reason=%s, want %s", dead[0].Reason, DeadMaxAttempts)
			}
			t.Logf("max attempts conclusion: seq=%d reason=%s attempts=%d",
				dead[0].Seq, dead[0].Reason, dead[0].Attempts)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("message not dead-lettered after max attempts")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestGroupConcurrentChurn 成员频繁上下线 + 多发布者并发 + 大量消息：
// 不丢消息、同键有序、已确认不再派、不拖住其他主题、无死锁/无 data race。
func TestGroupConcurrentChurn(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 32, ReliableEnqueueTimeoutMs: 5000,
		GroupMaxPending: 1024, GroupDeliveryTimeoutMs: 60000, GroupMaxAttempts: 1000}, nil)
	defer bus.Close()
	ctx := context.Background()

	const membersN = 3
	const publishers = 4
	const perPublisher = 50
	const total = publishers * perPublisher
	const keys = 8

	members := make([]*GroupMember, 0, membersN)
	ids := make([]string, 0, membersN)
	for i := 0; i < membersN; i++ {
		m, err := bus.JoinGroup(ctx, "t/churn", "exec", GroupOption{QueueLen: 32})
		if err != nil {
			t.Fatal(err)
		}
		members = append(members, m)
		ids = append(ids, m.ID())
	}

	// 消费侧统计：acked 集合、每键最新首投序号、滞留/重投副本计数。
	type stats struct {
		mu           sync.Mutex
		acked        map[uint64]bool
		lastFresh    map[string]uint64
		stale        int
		redeliveries int
	}
	st := &stats{acked: map[uint64]bool{}, lastFresh: map[string]uint64{}}
	stop := make(chan struct{})
	var cwg sync.WaitGroup
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
					st.mu.Lock()
					if st.acked[seq] {
						// 已确认后又读到的滞留副本（掉线前已入通道）：
						// 允许存在但绝不再处理，仅计数。
						st.stale++
						st.mu.Unlock()
						continue
					}
					if seq < st.lastFresh[d.Msg.Key] {
						t.Errorf("order violation: key %s seq=%d after %d",
							d.Msg.Key, seq, st.lastFresh[d.Msg.Key])
					} else if seq == st.lastFresh[d.Msg.Key] {
						st.redeliveries++ // 未确认被转派的重投副本
					} else {
						st.lastFresh[d.Msg.Key] = seq
					}
					st.mu.Unlock()
					// 处理耗时，让在途消息与成员掉线窗口重叠，
					// 从而真正走到“未确认工作被转派”的路径。
					time.Sleep(time.Millisecond)
					if err := m.Ack(seq); err == nil {
						st.mu.Lock()
						st.acked[seq] = true
						st.mu.Unlock()
					}
				case <-stop:
					return
				}
			}
		}(m)
	}

	// 成员上下线搅动。
	var churnWg sync.WaitGroup
	churnWg.Add(1)
	go func() {
		defer churnWg.Done()
		for i := 0; i < 30; i++ {
			idx := i % membersN
			_ = members[idx].Leave(ctx)
			time.Sleep(2 * time.Millisecond)
			if _, err := bus.JoinGroup(ctx, "t/churn", "exec",
				GroupOption{MemberID: ids[idx], QueueLen: 32}); err != nil {
				t.Errorf("rejoin member %s: %v", ids[idx], err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// 多发布者并发向该主题发布（带顺序键）。
	var pubWg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pubWg.Add(1)
		go func(pid int) {
			defer pubWg.Done()
			for i := 0; i < perPublisher; i++ {
				key := fmt.Sprintf("robot-%d", (pid*perPublisher+i)%keys)
				if _, err := bus.PublishRawKeyed(ctx, "t/churn", key, 1, QoSReliable,
					[]byte(fmt.Sprintf("p%d-%d", pid, i))); err != nil {
					t.Errorf("publisher %d: %v", pid, err)
					return
				}
			}
		}(p)
	}

	// 搅动期间其他主题发布者必须不被拖住。
	otherDone := make(chan struct{})
	go func() {
		defer close(otherDone)
		for i := 0; i < 50; i++ {
			if _, err := bus.PublishRaw(ctx, "t/unrelated", 1, QoSLowLatency, []byte{1}); err != nil {
				t.Errorf("unrelated publish: %v", err)
				return
			}
		}
	}()
	select {
	case <-otherDone:
		t.Logf("unrelated topic published 50 msgs during churn (no cross-topic blocking)")
	case <-time.After(3 * time.Second):
		t.Fatal("unrelated publisher blocked during group churn")
	}

	// 等待全部消息被确认（不丢）。
	deadline := time.After(20 * time.Second)
	for {
		st.mu.Lock()
		n := len(st.acked)
		st.mu.Unlock()
		if n == total {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout: acked %d/%d (messages lost or deadlock)", n, total)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	pubWg.Wait()
	churnWg.Wait()
	close(stop)
	cwg.Wait()

	// 判定：恰好 1..total 全部被确认一次。
	for seq := uint64(1); seq <= total; seq++ {
		if !st.acked[seq] {
			t.Fatalf("seq=%d never acked (lost)", seq)
		}
	}
	st.mu.Lock()
	t.Logf("churn session: %d msgs acked exactly once, redeliveries=%d stale-copies=%d, per-key order preserved",
		len(st.acked), st.redeliveries, st.stale)
	st.mu.Unlock()
}
