package pubsub

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentPublishOrder 多个发布者并发向同一可靠主题发布，
// 订阅者收到的消息必须严格按总线序号升序，且无数据竞争。
func TestConcurrentPublishOrder(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 256, ReliableEnqueueTimeoutMs: 3000,
		RedeliveryTimeoutMs: 60000}, nil)
	defer bus.Close()
	ctx := context.Background()
	sub, err := bus.Subscribe(ctx, "t/conc", QoSReliable, SubscribeOption{QueueLen: 256})
	if err != nil {
		t.Fatal(err)
	}

	const publishers = 8
	const perPublisher = 60
	total := publishers * perPublisher

	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				r, err := bus.PublishRaw(ctx, "t/conc", 1, QoSReliable,
					[]byte(fmt.Sprintf("p%d-%d", pid, i)))
				if err != nil {
					t.Errorf("publisher %d publish: %v", pid, err)
					return
				}
				// 每条都应投递到该在线可靠订阅者。
				if r.Delivered != 1 {
					t.Errorf("publisher %d seq=%d delivered=%d", pid, r.Seq, r.Delivered)
				}
			}
		}(p)
	}

	// 消费并立即 Ack，校验严格递增。
	received := make(map[uint64]bool, total)
	var lastSeq uint64
	deadline := time.After(10 * time.Second)
	for len(received) < total {
		select {
		case d := <-sub.Messages():
			if d.Msg.Seq <= lastSeq && len(received) > 0 {
				t.Fatalf("out-of-order/dup delivery: seq=%d after %d attempt=%d",
					d.Msg.Seq, lastSeq, d.Attempt)
			}
			if received[d.Msg.Seq] {
				t.Fatalf("duplicate delivery seq=%d", d.Msg.Seq)
			}
			received[d.Msg.Seq] = true
			lastSeq = d.Msg.Seq
			if err := sub.Ack(d.Msg.Seq); err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatalf("timeout: got %d/%d", len(received), total)
		}
	}
	wg.Wait()
	t.Logf("concurrent session: received=%d unique, lastSeq=%d, strictly ordered",
		len(received), lastSeq)
}

// TestReconnectNoLossNoDuplicate 订阅者离线期间发布的消息在重连后
// 必须全部续传，且已 Ack 的消息绝不重复。
func TestReconnectNoLossNoDuplicate(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 128, ReliableEnqueueTimeoutMs: 2000,
		RedeliveryTimeoutMs: 60000}, nil)
	defer bus.Close()
	ctx := context.Background()
	sub, _ := bus.Subscribe(ctx, "t/reconn", QoSReliable, SubscribeOption{QueueLen: 128})

	// 阶段 1：收 50 条并全部 Ack。
	for i := 0; i < 50; i++ {
		if _, err := bus.PublishRaw(ctx, "t/reconn", 1, QoSReliable, []byte("a")); err != nil {
			t.Fatal(err)
		}
	}
	ackd := map[uint64]bool{}
	for i := 0; i < 50; i++ {
		d := <-sub.Messages()
		if err := sub.Ack(d.Msg.Seq); err != nil {
			t.Fatal(err)
		}
		ackd[d.Msg.Seq] = true
	}

	// 阶段 2：离线期间发布 100 条。
	if err := sub.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := bus.PublishRaw(ctx, "t/reconn", 1, QoSReliable, []byte("b")); err != nil {
			t.Fatal(err)
		}
	}
	if err := sub.Resume(ctx); err != nil {
		t.Fatal(err)
	}

	// 阶段 3：重连后收到且仅收到 seq 51..150，顺序正确、无重复。
	got := map[uint64]bool{}
	var last uint64 = 50
	deadline := time.After(5 * time.Second)
	for len(got) < 100 {
		select {
		case d := <-sub.Messages():
			if ackd[d.Msg.Seq] {
				t.Fatalf("already-acked message redelivered: seq=%d", d.Msg.Seq)
			}
			if got[d.Msg.Seq] {
				t.Fatalf("duplicate after resume: seq=%d", d.Msg.Seq)
			}
			if d.Msg.Seq <= last {
				t.Fatalf("order broken: seq=%d after %d", d.Msg.Seq, last)
			}
			last = d.Msg.Seq
			got[d.Msg.Seq] = true
			_ = sub.Ack(d.Msg.Seq)
		case <-deadline:
			t.Fatalf("timeout got %d/100", len(got))
		}
	}
	t.Logf("reconnect session: acked-before=%d, resumed=%d, no loss/no duplicate",
		len(ackd), len(got))
}

// TestResumeByID 用订阅者 ID 在新句柄上续传：验证 ResumeID 语义。
func TestResumeByID(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 32, RedeliveryTimeoutMs: 60000}, nil)
	defer bus.Close()
	ctx := context.Background()
	s1, _ := bus.Subscribe(ctx, "t/id", QoSReliable, SubscribeOption{QueueLen: 32})
	id := s1.ID()
	// 收 1 条并 Ack。
	bus.PublishRaw(ctx, "t/id", 1, QoSReliable, []byte("x"))
	d := <-s1.Messages()
	_ = s1.Ack(d.Msg.Seq)
	// 模拟旧句柄离开（Pause），随后用相同 ID 重新订阅续传。
	_ = s1.Pause(ctx)
	bus.PublishRaw(ctx, "t/id", 1, QoSReliable, []byte("y"))

	s2, err := bus.Subscribe(ctx, "t/id", QoSReliable, SubscribeOption{ResumeID: id})
	if err != nil {
		t.Fatal(err)
	}
	if s2.ID() != id {
		t.Fatalf("resume id mismatch %s != %s", s2.ID(), id)
	}
	d2 := <-s2.Messages()
	if d2.Msg.Seq != 2 {
		t.Fatalf("expected resumed seq=2 got %d", d2.Msg.Seq)
	}
	t.Logf("resume-by-id ok: subID=%s resumedSeq=%d", id, d2.Msg.Seq)
}

// TestSubscriptionVisibility 订阅关系增删对发布立即可见：
// 退订后再发布应得到无订阅者结论。
func TestSubscriptionVisibility(t *testing.T) {
	bus := New(BusConfig{}, nil)
	defer bus.Close()
	ctx := context.Background()
	sub, _ := bus.Subscribe(ctx, "t/vis", QoSLowLatency, SubscribeOption{QueueLen: 4})
	r, _ := bus.PublishRaw(ctx, "t/vis", 1, QoSLowLatency, []byte{1})
	if r.Delivered != 1 {
		t.Fatalf("expected delivered 1, got %+v", r)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	r2, err := bus.PublishRaw(ctx, "t/vis", 1, QoSLowLatency, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	if len(r2.Dropped) != 1 || r2.Dropped[0].Reason != DropNoSubscriber {
		t.Fatalf("subscription removal not visible: %+v", r2.Dropped)
	}
	// 已关闭订阅再 Ack 应得到明确错误。
	if err := sub.Ack(1); err == nil {
		t.Fatal("expected error on Ack after close")
	}
	t.Logf("subscription add/remove immediately visible: %s", r2.Dropped[0].Reason)
}

// TestConcurrentOverflowStress 并发高可靠发布 + 慢消费者 + 队列溢出，
// 验证不丢已入队消息、不死锁、最终全部可在消费者跟上后收齐。
func TestConcurrentOverflowStress(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 16, ReliableEnqueueTimeoutMs: 100,
		RedeliveryTimeoutMs: 60000}, nil)
	defer bus.Close()
	ctx := context.Background()
	sub, _ := bus.Subscribe(ctx, "t/stress", QoSReliable, SubscribeOption{QueueLen: 16})

	const N = 200
	var wg sync.WaitGroup
	var overflow, success int64
	var mu sync.Mutex
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			if _, err := bus.PublishRaw(ctx, "t/stress", 1, QoSReliable, []byte{byte(i)}); err != nil {
				if IsError(err, KindOverflow) {
					mu.Lock()
					overflow++
					mu.Unlock()
					// 背压：让出并重试直到成功（模拟发布者被反馈后等待）。
					time.Sleep(5 * time.Millisecond)
					i--
					continue
				}
				return
			}
			mu.Lock()
			success++
			mu.Unlock()
		}
	}()

	// 慢消费者：逐步消费并 Ack，最终必须收齐 N 条，无重复。
	got := map[uint64]bool{}
	deadline := time.After(15 * time.Second)
	for len(got) < N {
		select {
		case d := <-sub.Messages():
			if got[d.Msg.Seq] {
				t.Fatalf("duplicate seq=%d", d.Msg.Seq)
			}
			got[d.Msg.Seq] = true
			time.Sleep(time.Millisecond) // 制造背压
			_ = sub.Ack(d.Msg.Seq)
		case <-deadline:
			t.Fatalf("timeout got %d/%d (overflow=%d success=%d)",
				len(got), N, overflow, success)
		}
	}
	wg.Wait()
	t.Logf("stress session: delivered=%d overflow-retries=%d, all %d collected exactly once",
		success, overflow, len(got))
}
