package pubsub

import (
	"context"
	"testing"
	"time"
)

// TestLowLatencyDropsWithReason 低延迟等级：慢消费者队列满时必须丢弃，
// 且 PublishResult.Dropped 给出 DropQueueFull；发布绝不阻塞。
func TestLowLatencyDropsWithReason(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 4}, nil)
	defer bus.Close()
	ctx := context.Background()
	sub, err := bus.Subscribe(ctx, "t/ll", QoSLowLatency, SubscribeOption{QueueLen: 4})
	if err != nil {
		t.Fatal(err)
	}
	var fullDrops, delivered int
	for i := 0; i < 20; i++ {
		r, err := bus.PublishRaw(ctx, "t/ll", 1, QoSLowLatency, []byte{byte(i)})
		if err != nil {
			t.Fatalf("low latency publish must not block/error: %v", err)
		}
		for _, d := range r.Dropped {
			if d.Reason == DropQueueFull {
				fullDrops++
			}
		}
		delivered += r.Delivered
	}
	if fullDrops == 0 {
		t.Fatal("expected some DropQueueFull, got none")
	}
	t.Logf("low-latency: delivered=%d queue-full drops=%d (sub=%s)", delivered, fullDrops, sub.ID())
	if delivered+fullDrops != 20 {
		t.Fatalf("accounting mismatch delivered=%d drops=%d", delivered, fullDrops)
	}
}

// TestLowLatencyNoSubscriber 无订阅者时给出 DropNoSubscriber 而不是报错/丢失静默。
func TestLowLatencyNoSubscriber(t *testing.T) {
	bus := New(BusConfig{}, nil)
	defer bus.Close()
	r, err := bus.PublishRaw(context.Background(), "t/none", 1, QoSLowLatency, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Dropped) != 1 || r.Dropped[0].Reason != DropNoSubscriber {
		t.Fatalf("expected single DropNoSubscriber, got %+v", r.Dropped)
	}
	t.Logf("no-subscriber conclusion: %s", r.Dropped[0].Reason)
}

// TestReliableOverflowAndIsolation 可靠等级：慢订阅者背压超时返回
// KindOverflow；并且该阻塞不影响其他主题的发布（无死锁、不连带阻塞）。
func TestReliableOverflowAndIsolation(t *testing.T) {
	bus := New(BusConfig{
		DefaultQueueLen:          4,
		ReliableEnqueueTimeoutMs: 120,
	}, nil)
	defer bus.Close()
	ctx := context.Background()

	// 慢订阅者：从不 Ack，填满 4 个未完成窗口。
	slow, err := bus.Subscribe(ctx, "t/slow", QoSReliable, SubscribeOption{QueueLen: 4})
	if err != nil {
		t.Fatal(err)
	}
	// 排空投递通道但永不 Ack，使 pending 累积。
	go func() {
		for range slow.Messages() {
		}
	}()

	// 填满窗口（4 条入队成功，第 5 条起等待）。
	for i := 0; i < 4; i++ {
		if _, err := bus.PublishRaw(ctx, "t/slow", 1, QoSReliable, []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	_, err = bus.PublishRaw(ctx, "t/slow", 1, QoSReliable, []byte{1})
	if err == nil || !IsError(err, KindOverflow) {
		t.Fatalf("expected KindOverflow, got %v", err)
	}
	t.Logf("backpressure conclusion: %v", err)

	// 无关主题必须仍然可立即发布成功（不被慢订阅者连带阻塞）。
	done := make(chan error, 1)
	go func() {
		_, e := bus.PublishRaw(ctx, "t/other", 1, QoSLowLatency, []byte{9})
		done <- e
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("unrelated publish failed: %v", e)
		}
		t.Logf("unrelated topic publish unaffected (isolation OK)")
	case <-time.After(time.Second):
		t.Fatal("unrelated publisher blocked: deadlock/isolation violation")
	}
}
