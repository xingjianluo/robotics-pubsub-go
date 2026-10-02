package pubsub

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// execRecv 在超时时间内从执行器通道取一条投递。
func execRecv(e *Executor, d time.Duration) (*Delivery, error) {
	select {
	case x, ok := <-e.Messages():
		if !ok || x == nil {
			return nil, fmt.Errorf("executor %s channel closed", e.ID())
		}
		return x, nil
	case <-time.After(d):
		return nil, fmt.Errorf("executor %s recv timeout", e.ID())
	}
}

func fastGroupCfg() GroupConfig {
	return GroupConfig{
		MaxBacklog:       256,
		ExecutorQueueLen: 64,
		ProcessTimeoutMs: 60000,
		WaitTimeoutMs:    60000,
		MaxAttempts:      5,
	}
}

// TestExecCompetingConsumers 场景一：同主题同组挂多个执行器并发分担。
//
// 判定依据：
//  1. 每条消息的每一次派发都只落在唯一一个执行器上（同一条不会被
//     几个执行器同时拿走）：首次派发去重 + 执行器归属唯一；
//  2. N 条消息恰好被处理 N 次（不丢不重），且至少两个执行器分到活，
//     证明是“分担”而不是固定单点。
func TestExecCompetingConsumers(t *testing.T) {
	bus := New(BusConfig{}, nil)
	defer bus.Close()
	ctx := context.Background()
	g, err := bus.CreateExecutorGroup(ctx, "robot/tasks", "workers", fastGroupCfg())
	if err != nil {
		t.Fatal(err)
	}
	const E = 3
	execs := make([]*Executor, E)
	for i := range execs {
		execs[i], err = g.Join(ctx, ExecutorOption{})
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("[competing] group=%s online executors=%d, expect load shared across executors", g.Name(), E)

	const N = 60
	for i := 1; i <= N; i++ {
		if _, err := bus.PublishRawToGroup(ctx, "robot/tasks", "workers", 1,
			QoSReliable, []byte{byte(i)}, fmt.Sprintf("task-%d", i)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	owner := map[uint64]string{} // seq -> 首次接手的执行器（唯一）
	perExec := map[string]int{}
	var wg sync.WaitGroup
	wg.Add(E)
	for _, e := range execs {
		go func(e *Executor) {
			defer wg.Done()
			for {
				d, err := execRecv(e, time.Second)
				if err != nil {
					return
				}
				mu.Lock()
				if old, dup := owner[d.Msg.Seq]; dup {
					mu.Unlock()
					t.Errorf("seq=%d handed to %s but already owned by %s (double hand-out)",
						d.Msg.Seq, e.ID(), old)
					return
				}
				owner[d.Msg.Seq] = e.ID()
				perExec[e.ID()]++
				done := len(owner)
				mu.Unlock()
				if err := e.Ack(d.Msg.Seq); err != nil {
					t.Errorf("ack: %v", err)
					return
				}
				if done >= N {
					return
				}
			}
		}(e)
	}
	wg.Wait()

	if len(owner) != N {
		t.Fatalf("expected %d distinct messages handled, got %d", N, len(owner))
	}
	sharing := 0
	for _, n := range perExec {
		if n > 0 {
			sharing++
		}
	}
	t.Logf("[competing] handled=%d, per-executor distribution=%v, sharing executors=%d",
		len(owner), perExec, sharing)
	if sharing < 2 {
		t.Fatalf("work not shared: only %d executor(s) received messages", sharing)
	}
}

// TestExecFailoverOnLeave 场景二：执行器中途掉线，其已接手但未 Ack 的活
// 必须转给还在线的执行器；主题不被堵死；未确认消息不丢、已确认不重派。
func TestExecFailoverOnLeave(t *testing.T) {
	bus := New(BusConfig{RedeliveryTimeoutMs: 60000}, nil)
	defer bus.Close()
	ctx := context.Background()
	g, err := bus.CreateExecutorGroup(ctx, "robot/cells", "line-1", fastGroupCfg())
	if err != nil {
		t.Fatal(err)
	}
	a, _ := g.Join(ctx, ExecutorOption{QueueLen: 4})
	b, _ := g.Join(ctx, ExecutorOption{QueueLen: 4})
	t.Logf("[failover] executors online: %s, %s; publishing 8 messages on one key", a.ID(), b.ID())

	const N = 8
	for i := 1; i <= N; i++ {
		if _, err := bus.PublishRawToGroup(ctx, "robot/cells", "line-1", 1,
			QoSReliable, []byte{byte(i)}, "cell-A"); err != nil {
			t.Fatal(err)
		}
	}

	// 同一 Key：同一时刻只有队头在一个执行器手上。A 接手 seq=1。
	d1, err := execRecv(a, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if d1.Msg.Seq != 1 {
		t.Fatalf("expected first hand-out seq=1, got %d", d1.Msg.Seq)
	}
	// A 完成 seq=1：已确认消息后面绝不能再出现。
	if err := a.Ack(1); err != nil {
		t.Fatal(err)
	}
	t.Logf("[failover] seq=1 handed to and acked by %s", a.ID())

	// 队头 seq=2 由 B 接手，但 B 在确认前掉线。
	d2, err := execRecv(b, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if d2.Msg.Seq != 2 {
		t.Fatalf("expected seq=2 handed to %s, got %d", b.ID(), d2.Msg.Seq)
	}
	if err := b.Leave(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("[failover] %s left holding unacked seq=%d; expecting hand-over to online %s",
		b.ID(), 2, a.ID())

	// B 掉线后 seq=2 必须被在线的 A 接手（重复投递副本，但只 A 一个人拿到）。
	red, err := execRecv(a, 2*time.Second)
	if err != nil {
		t.Fatalf("unacked work was not handed over after executor leave: %v", err)
	}
	if red.Msg.Seq != 2 {
		t.Fatalf("expected hand-over seq=2, got %d (key queue must not skip unacked head)", red.Msg.Seq)
	}
	if red.Attempt < 2 {
		t.Fatalf("handed-over delivery must be a retry, attempt=%d", red.Attempt)
	}
	t.Logf("[failover] seq=2 handed over to %s attempt=%d (unacked not lost)", a.ID(), red.Attempt)
	if err := a.Ack(2); err != nil {
		t.Fatal(err)
	}

	// B 在 A 干活期间以相同 ID 回来。
	b2, err := g.Join(ctx, ExecutorOption{ResumeID: b.ID(), QueueLen: 4})
	if err != nil {
		t.Fatal(err)
	}
	if b2.ID() != b.ID() {
		t.Fatal("resume id mismatch")
	}
	t.Logf("[failover] %s rejoined with same id; both executors drain the rest", b2.ID())

	// 两个执行器一起把剩余活干完；全部消息恰好完成一次。
	completed := map[uint64]bool{1: true, 2: true}
	drain := map[uint64]string{} // seq -> 当前在途执行器
	seen := map[uint64]bool{}    // 派发出去过（含转接副本）

	deadline := time.After(5 * time.Second)
	for len(completed) < N {
		select {
		case d := <-a.Messages():
			recordDelivery(t, "failover", d, a, seen, drain, completed)
			if err := a.Ack(d.Msg.Seq); err != nil {
				t.Fatal(err)
			}
			complete(drain, completed, d.Msg.Seq, a.ID())
		case d := <-b2.Messages():
			recordDelivery(t, "failover", d, b2, seen, drain, completed)
			if err := b2.Ack(d.Msg.Seq); err != nil {
				t.Fatal(err)
			}
			complete(drain, completed, d.Msg.Seq, b2.ID())
		case <-deadline:
			t.Fatalf("timeout: completed=%d/%d drain=%v", len(completed), N, drain)
		}
	}
	if s := g.Stats(); s.Retained != 0 || s.InFlight != 0 || s.DeadLetters != 0 {
		t.Fatalf("unexpected group state: %+v", s)
	}
	t.Logf("[failover] all %d messages completed exactly once; seq=1 never re-dispatched; retained=0", N)
}

// recordDelivery 校验一条派发的合法性：同一条消息在其未完成期间不得
// 同时落在两个执行器手上；已完成的消息不得再派发。
func recordDelivery(t *testing.T, tag string, d *Delivery, e *Executor,
	seen map[uint64]bool, drain map[uint64]string, completed map[uint64]bool) {
	t.Helper()
	if completed[d.Msg.Seq] {
		t.Fatalf("[%s] already-completed seq=%d re-dispatched to %s", tag, d.Msg.Seq, e.ID())
	}
	if owner, busy := drain[d.Msg.Seq]; busy && owner != e.ID() {
		t.Fatalf("[%s] seq=%d handed to %s while still held by %s (concurrent double hand-out)",
			tag, d.Msg.Seq, e.ID(), owner)
	}
	seen[d.Msg.Seq] = true
	drain[d.Msg.Seq] = e.ID()
}

// complete 把消息标记完成：从在途表移除并记录已完成集合。
func complete(drain map[uint64]string, completed map[uint64]bool, seq uint64, by string) {
	delete(drain, seq)
	completed[seq] = true
}

// TestExecPerKeyOrderAcrossSwitch 场景三：同一机器人(Key)的消息必须保持
// 原本先后顺序，即使处理它的执行器中途换了人、成员数量变了；不同 Key
// 之间允许并行分担。
func TestExecPerKeyOrderAcrossSwitch(t *testing.T) {
	bus := New(BusConfig{}, nil)
	defer bus.Close()
	ctx := context.Background()
	g, err := bus.CreateExecutorGroup(ctx, "robot/move", "movers", GroupConfig{
		MaxBacklog: 512, ExecutorQueueLen: 32,
		ProcessTimeoutMs: 60000, WaitTimeoutMs: 60000, MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	e1, _ := g.Join(ctx, ExecutorOption{QueueLen: 32})
	e2, _ := g.Join(ctx, ExecutorOption{QueueLen: 32})

	keys := []string{"robot-1", "robot-2", "robot-3"}
	// 每个 key 8 条，交错发布；同 key 序号即 1..8。
	type pub struct {
		key string
		n   int
	}
	var pubs []pub
	for n := 1; n <= 8; n++ {
		for _, k := range keys {
			pubs = append(pubs, pub{k, n})
		}
	}
	// 共用主题序号空间，Key 内顺序用消息体中的 n（key#n）判定，
	// 主题 Seq 仍用于 Ack 去重。
	for _, p := range pubs {
		if _, err := bus.PublishRawToGroup(ctx, "robot/move", "movers", 1,
			QoSReliable, []byte(fmt.Sprintf("%s#%d", p.key, p.n)), p.key); err != nil {
			t.Fatal(err)
		}
	}

	// tracker 需要按“key 内序号”判定，observe 传入 n 而非主题 Seq。
	type ordTracker struct {
		mu       sync.Mutex
		next     map[string]int
		inflight map[string]string // "key@n" -> exec
		done     map[string]bool
	}
	tr := &ordTracker{
		next:     map[string]int{},
		inflight: map[string]string{},
		done:     map[string]bool{},
	}
	for _, k := range keys {
		tr.next[k] = 1
	}
	check := func(d *Delivery, e *Executor) {
		// 从 body 解析 key#n
		body := string(d.Msg.Payload)
		k, ns, ok := strings.Cut(body, "#")
		n64, err := strconv.Atoi(ns)
		if !ok || err != nil {
			t.Fatalf("bad body %q", body)
		}
		n := n64
		tr.mu.Lock()
		defer tr.mu.Unlock()
		id := fmt.Sprintf("%s@%d", k, n)
		if tr.done[id] {
			t.Fatalf("order: completed %s re-dispatched", id)
		}
		if owner, busy := tr.inflight[id]; busy && owner != e.ID() {
			t.Fatalf("order: %s concurrently on %s and %s", id, owner, e.ID())
		}
		switch want := tr.next[k]; {
		case n == want:
			tr.next[k] = n + 1
			tr.inflight[id] = e.ID()
		case n < want:
			if d.Attempt < 2 {
				t.Fatalf("order: %s first-delivery out of order (want %d)", id, want)
			}
			tr.inflight[id] = e.ID() // 合法转接副本
		default:
			t.Fatalf("order: %s jumped ahead of expected %d despite executor switch", id, want)
		}
	}

	const total = 24
	completed := make(chan uint64, total)
	var wg sync.WaitGroup
	consume := func(e *Executor) {
		defer wg.Done()
		for {
			select {
			case d, ok := <-e.Messages():
				if !ok {
					return
				}
				check(d, e)
				_ = e.Ack(d.Msg.Seq)
				k2, ns2, _ := strings.Cut(string(d.Msg.Payload), "#")
				n2, _ := strconv.Atoi(ns2)
				id := fmt.Sprintf("%s@%d", k2, n2)
				tr.mu.Lock()
				delete(tr.inflight, id)
				tr.done[id] = true
				tr.mu.Unlock()
				completed <- d.Msg.Seq
			case <-time.After(1500 * time.Millisecond):
				return
			}
		}
	}
	wg.Add(2)
	go consume(e1)
	go consume(e2)

	// 运行中途：e2 离开、再加两个新执行器（成员数量变化），验证顺序不被打乱。
	time.Sleep(60 * time.Millisecond)
	_ = e2.Leave(ctx)
	t.Logf("[order] membership change: %s left mid-stream", e2.ID())
	e3, _ := g.Join(ctx, ExecutorOption{QueueLen: 32})
	e4, _ := g.Join(ctx, ExecutorOption{QueueLen: 32})
	wg.Add(2)
	go consume(e3)
	go consume(e4)

	got := 0
	deadline := time.After(8 * time.Second)
	for got < total {
		select {
		case <-completed:
			got++
		case <-deadline:
			t.Fatalf("order: only completed %d/%d", got, total)
		}
	}
	wg.Wait()
	if s := g.Stats(); s.Retained != 0 {
		t.Fatalf("order: retained=%d after drain", s.Retained)
	}
	tr.mu.Lock()
	logNext := map[string]int{}
	for k, v := range tr.next {
		logNext[k] = v - 1
	}
	tr.mu.Unlock()
	t.Logf("[order] all %d messages per-key strictly ordered across leave/join: %v; executors 2->1->3",
		total, logNext)
}

// TestExecConcurrentChurnAndOverflow 场景四：成员频繁上下线 + 消息大量
// 堆积时：不死锁、不静默丢失、每条恰好完成一次；积压超限时可靠发布得到
// KindOverflow、低延迟发布得到 DropQueueFull，且消息都进死信队列可查。
func TestExecConcurrentChurnAndOverflow(t *testing.T) {
	bus := New(BusConfig{}, nil)
	defer bus.Close()
	ctx := context.Background()
	const cap2 = 10
	g, err := bus.CreateExecutorGroup(ctx, "robot/batch", "pool", GroupConfig{
		MaxBacklog: cap2, ExecutorQueueLen: 4,
		ProcessTimeoutMs: 60000, WaitTimeoutMs: 60000, MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 先不挂执行器，制造堆积并验证超限的两种可区分结论。
	var overflow, fullDrops, enqueued int
	publishOne := func(qos QoS) {
		r, err := bus.PublishRawToGroup(ctx, "robot/batch", "pool", 1, qos,
			[]byte("x"), "k")
		switch {
		case qos == QoSReliable && IsError(err, KindOverflow):
			overflow++
		case qos == QoSReliable && err != nil:
			t.Errorf("unexpected reliable err: %v", err)
		case qos == QoSLowLatency && err == nil && len(r.Dropped) == 1 &&
			r.Dropped[0].Reason == DropQueueFull:
			fullDrops++
		case err == nil:
			enqueued++
		default:
			t.Errorf("unexpected publish result err=%v res=%+v", err, r)
		}
	}
	for i := 0; i < cap2; i++ {
		publishOne(QoSReliable) // 前 10 条入保留队列
	}
	for i := 0; i < 5; i++ {
		publishOne(QoSReliable) // 5 条溢出 -> 死信
	}
	for i := 0; i < 3; i++ {
		publishOne(QoSLowLatency) // 3 条满丢弃 -> 同死信，原因一致为积压
	}
	if enqueued != cap2 || overflow != 5 || fullDrops != 3 {
		t.Fatalf("backlog accounting: enqueued=%d overflow=%d fullDrops=%d",
			enqueued, overflow, fullDrops)
	}
	if s := g.Stats(); s.Retained != cap2 || s.DeadLetters != 8 {
		t.Fatalf("unexpected stats before drain: %+v", s)
	}
	t.Logf("[churn] backlog full at %d: reliable KindOverflow=%d, low-latency DropQueueFull=%d; all %d in dead-letter",
		cap2, overflow, fullDrops, g.Stats().DeadLetters)

	// 挂 3 个执行器并频繁上下线，收齐前 cap2 条保留消息。
	// 用 slotMu 保护“槽位 -> 当前句柄”：churn 替换句柄后，消费协程
	// 下一轮读取自然拿到新句柄；旧通道关闭仅结束本次 select。
	var slotMu [3]sync.Mutex
	var current [3]*Executor
	for i := 0; i < 3; i++ {
		e, _ := g.Join(ctx, ExecutorOption{QueueLen: 4})
		current[i] = e
	}
	stop := make(chan struct{})
	var churnWG sync.WaitGroup
	churnWG.Add(1)
	go func() {
		defer churnWG.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(30 * time.Millisecond):
				idx := int(time.Now().UnixNano()) % 3
				slotMu[idx].Lock()
				old := current[idx]
				id := old.ID()
				_ = old.Leave(ctx)
				time.Sleep(10 * time.Millisecond)
				ne, err := g.Join(ctx, ExecutorOption{ResumeID: id, QueueLen: 4})
				if err == nil {
					current[idx] = ne
				}
				slotMu[idx].Unlock()
			}
		}
	}()

	var wg sync.WaitGroup
	handled := make(chan uint64, cap2)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				slotMu[slot].Lock()
				e := current[slot]
				slotMu[slot].Unlock()
				select {
				case d, ok := <-e.Messages():
					if !ok || d == nil {
						time.Sleep(5 * time.Millisecond) // 通道被 resume 重建
						continue
					}
					_ = e.Ack(d.Msg.Seq)
					select {
					case handled <- d.Msg.Seq:
					case <-stop:
						return
					}
				case <-time.After(50 * time.Millisecond):
				case <-stop:
					return
				}
			}
		}(i)
	}
	uniq := map[uint64]bool{}
	deadline := time.After(10 * time.Second)
	for len(uniq) < cap2 {
		select {
		case s := <-handled:
			if uniq[s] {
				t.Fatalf("message seq=%d processed twice under churn", s)
			}
			uniq[s] = true
		case <-deadline:
			t.Fatalf("churn drain timeout: %d/%d", len(uniq), cap2)
		}
	}
	close(stop)
	wg.Wait()
	churnWG.Wait()
	t.Logf("[churn] retained %d drained exactly once with executors flapping; dead letters still %d",
		len(uniq), g.Stats().DeadLetters)
	if g.Stats().DeadLetters != 8 {
		t.Fatalf("dead letters must stay available: %d", g.Stats().DeadLetters)
	}
}

// TestExecDeadLetterRequeue 场景五：死信可查看、可重新投递；重投同样
// 满足同 Key 顺序与不重复，且重投时积压仍满会得到 KindOverflow 且死信保留。
func TestExecDeadLetterRequeue(t *testing.T) {
	bus := New(BusConfig{}, nil)
	defer bus.Close()
	ctx := context.Background()
	g, _ := bus.CreateExecutorGroup(ctx, "robot/dlq", "g", GroupConfig{
		MaxBacklog: 2, ExecutorQueueLen: 8,
		ProcessTimeoutMs: 60000, WaitTimeoutMs: 60000, MaxAttempts: 5,
	})
	// 积压 2 条满，第 3 条溢出进死信。
	r1, err := bus.PublishRawToGroup(ctx, "robot/dlq", "g", 1, QoSReliable, []byte("a"), "k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bus.PublishRawToGroup(ctx, "robot/dlq", "g", 1, QoSReliable, []byte("b"), "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.PublishRawToGroup(ctx, "robot/dlq", "g", 1, QoSReliable, []byte("c"), "k"); !IsError(err, KindOverflow) {
		t.Fatalf("expected KindOverflow, got %v", err)
	}
	dls := g.DeadLetters()
	if len(dls) != 1 || dls[0].Reason != DeadBacklogOverflow {
		t.Fatalf("expected 1 overflow dead letter, got %+v", dls)
	}
	deadSeq := dls[0].Msg.Seq
	t.Logf("[dlq] overflowed seq=%d viewable in dead-letter queue reason=%s",
		deadSeq, dls[0].Reason)

	// 积压仍满时重投：必须 KindOverflow，且死信保留不丢。
	if err := g.RequeueDead(ctx, deadSeq); !IsError(err, KindOverflow) {
		t.Fatalf("requeue while full expected KindOverflow, got %v", err)
	}
	if len(g.DeadLetters()) != 1 {
		t.Fatal("dead letter must be retained when requeue rejected")
	}

	// 挂执行器：先消费并 Ack 前 2 条（同 Key 顺序 1,2），再重投死信，
	// 它应被派发到执行器；随后新发布同 Key 消息排在它之后。
	e, _ := g.Join(ctx, ExecutorOption{QueueLen: 8})
	d, err := execRecv(e, time.Second)
	if err != nil || d.Msg.Seq != r1.Seq {
		t.Fatalf("first of key expected %d got %+v err=%v", r1.Seq, d, err)
	}
	_ = e.Ack(d.Msg.Seq)
	d, err = execRecv(e, time.Second)
	if err != nil || d.Msg.Seq != r1.Seq+1 {
		t.Fatalf("second of key expected %d got %+v err=%v", r1.Seq+1, d, err)
	}
	_ = e.Ack(d.Msg.Seq)

	if err := g.RequeueDead(ctx, deadSeq); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	// 死信重投排在其 Key 队列最前，应先于之后新发布的同 Key 消息。
	r4, err := bus.PublishRawToGroup(ctx, "robot/dlq", "g", 1, QoSReliable, []byte("d"), "k")
	if err != nil {
		t.Fatal(err)
	}
	d, err = execRecv(e, time.Second)
	if err != nil || d.Msg.Seq != deadSeq {
		t.Fatalf("requeued dead letter expected first, got %+v err=%v", d, err)
	}
	if d.Attempt != 1 {
		t.Fatalf("requeued delivery must restart attempts, got %d", d.Attempt)
	}
	_ = e.Ack(d.Msg.Seq)
	d, err = execRecv(e, time.Second)
	if err != nil || d.Msg.Seq != r4.Seq {
		t.Fatalf("new same-key message must follow requeued head, got %+v", d)
	}
	_ = e.Ack(d.Msg.Seq)
	if len(g.DeadLetters()) != 0 || g.Stats().Retained != 0 {
		t.Fatalf("dlq/retained not cleared: %+v", g.Stats())
	}
	t.Logf("[dlq] dead seq=%d redelivered before new seq=%d (key order), attempts reset; dlq drained",
		deadSeq, r4.Seq)
}

// TestExecTimeoutReasons 场景六：等待超时与处理超时必须给出彼此可区分、
// 且与积压溢出不同的死信原因；处理超时先转接、超过最大尝试才进死信。
func TestExecTimeoutReasons(t *testing.T) {
	bus := New(BusConfig{}, nil)
	defer bus.Close()
	ctx := context.Background()

	// 1) 等待超时：组内无执行器，消息排队超过 WaitTimeoutMs。
	gw, _ := bus.CreateExecutorGroup(ctx, "robot/wait", "g", GroupConfig{
		MaxBacklog: 8, ExecutorQueueLen: 4,
		ProcessTimeoutMs: 60000, WaitTimeoutMs: 120, MaxAttempts: 5,
	})
	if _, err := bus.PublishRawToGroup(ctx, "robot/wait", "g", 1, QoSReliable, []byte("w"), "k"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	dls := gw.DeadLetters()
	if len(dls) != 1 || dls[0].Reason != DeadWaitTimeout {
		t.Fatalf("expected DeadWaitTimeout, got %+v", dls)
	}
	t.Logf("[timeout] message never picked up -> %s (distinct from overflow/process)",
		dls[0].Reason)

	// 2) 处理超时：执行器接手后不 Ack；短超时 + MaxAttempts=2：
	//    第一次超时应先转接到（仍在线的）另一个执行器，第 2 次超时进死信。
	gp, _ := bus.CreateExecutorGroup(ctx, "robot/work", "g", GroupConfig{
		MaxBacklog: 8, ExecutorQueueLen: 4,
		ProcessTimeoutMs: 120, WaitTimeoutMs: 60000, MaxAttempts: 2,
	})
	x, _ := gp.Join(ctx, ExecutorOption{QueueLen: 4})
	y, _ := gp.Join(ctx, ExecutorOption{QueueLen: 4})
	if _, err := bus.PublishRawToGroup(ctx, "robot/work", "g", 1, QoSReliable, []byte("p"), "k"); err != nil {
		t.Fatal(err)
	}
	first, err := execRecv(x, time.Second)
	if err != nil {
		// round-robin 可能先给 y：交换角色。
		first, err = execRecv(y, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		x, y = y, x
	}
	t.Logf("[timeout] seq=%d handed to %s attempt=%d, not acked; waiting process timeout",
		first.Msg.Seq, x.ID(), first.Attempt)
	// 转接副本应在另一个在线执行器到达（attempt=2）。
	red, err := execRecv(y, time.Second)
	if err != nil {
		t.Fatalf("expected process-timeout hand-over to other executor: %v", err)
	}
	if red.Msg.Seq != first.Msg.Seq || red.Attempt != 2 {
		t.Fatalf("hand-over mismatch: %+v vs first seq=%d", red, first.Msg.Seq)
	}
	t.Logf("[timeout] seq=%d handed over to %s attempt=%d (retry before dead-letter)",
		red.Msg.Seq, y.ID(), red.Attempt)
	// 第二次处理超时（达到 MaxAttempts）-> 死信 DeadProcessTimeout。
	time.Sleep(400 * time.Millisecond)
	dls = gp.DeadLetters()
	if len(dls) != 1 || dls[0].Reason != DeadProcessTimeout || dls[0].Attempts != 2 {
		t.Fatalf("expected DeadProcessTimeout attempts=2, got %+v", dls)
	}
	t.Logf("[timeout] after max attempts -> %s attempts=%d (distinct reason)",
		dls[0].Reason, dls[0].Attempts)
}

// TestExecGroupTopicIsolation 场景七：某组成员频繁上下线、大量堆积时，
// 不能拖住其他主题的发布者。
func TestExecGroupTopicIsolation(t *testing.T) {
	bus := New(BusConfig{}, nil)
	defer bus.Close()
	ctx := context.Background()
	// 堆积主题：从不挂执行器，很快堆满，持续产生溢出死信。
	g, _ := bus.CreateExecutorGroup(ctx, "robot/jammed", "g", GroupConfig{
		MaxBacklog: 4, ExecutorQueueLen: 2,
		ProcessTimeoutMs: 60000, WaitTimeoutMs: 60000, MaxAttempts: 5,
	})
	for i := 0; i < 4; i++ {
		if _, err := bus.PublishRawToGroup(ctx, "robot/jammed", "g", 1, QoSReliable, []byte("x"), "k"); err != nil {
			t.Fatal(err)
		}
	}

	// 无关主题（普通发布 + 另一个组）必须始终快速成功，不互相等待卡死。
	done := make(chan string, 2)
	go func() {
		_, e := bus.PublishRaw(ctx, "robot/other", 1, QoSLowLatency, []byte{1})
		if e != nil {
			done <- "err:" + e.Error()
			return
		}
		done <- "plain-topic-ok"
	}()
	go func() {
		g2, e := bus.CreateExecutorGroup(ctx, "robot/other", "g2", fastGroupCfg())
		if e != nil {
			done <- "err:" + e.Error()
			return
		}
		if _, e := g2.Join(ctx, ExecutorOption{}); e != nil {
			done <- "err:" + e.Error()
			return
		}
		if _, e := bus.PublishRawToGroup(ctx, "robot/other", "g2", 1, QoSReliable, []byte{1}, "k"); e != nil {
			done <- "err:" + e.Error()
			return
		}
		done <- "other-group-ok"
	}()
	for i := 0; i < 2; i++ {
		select {
		case r := <-done:
			if len(r) > 4 && r[:4] == "err:" {
				t.Fatalf("unrelated topic blocked/failed while jammed group overflowing: %s", r)
			}
			t.Logf("[isolation] %s despite jammed backlog", r)
		case <-time.After(2 * time.Second):
			t.Fatal("unrelated publisher blocked: deadlock/isolation violation")
		}
	}

	// 继续往堆满的组发，确认立即返回 KindOverflow 而不是等待。
	started := time.Now()
	_, err := bus.PublishRawToGroup(ctx, "robot/jammed", "g", 1, QoSReliable, []byte("x"), "k")
	if !IsError(err, KindOverflow) {
		t.Fatalf("expected overflow, got %v", err)
	}
	if d := time.Since(started); d > 200*time.Millisecond {
		t.Fatalf("overflowing publish blocked for %s", d)
	}
	t.Logf("[isolation] jammed-group publish fails fast with KindOverflow in %v; dead letters=%d",
		time.Since(started), len(g.DeadLetters()))
}
