package pubsub

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLowLatency_QueueFullDropsAreExplainable(t *testing.T) {
	const depth = 4
	bus := newTestBus(t, WithQueueDepth(depth), WithDropPolicy(DropNewest))
	t.Cleanup(func() { bus.Close() })

	releaseDispatch := make(chan struct{})
	dispatchStarted := make(chan struct{}, 1)
	var gateOnce sync.Once
	h := func(_ context.Context, d *Delivery) { d.Ack() }
	sub, err := bus.Subscribe("c1", "telemetry", QoSLowLatency, h)
	if err != nil {
		t.Fatal(err)
	}
	// 派发循环在取出第一条前挂起，期间所有发布只能进队列 -> 确定性溢出。
	sub.preDispatch = func() {
		gateOnce.Do(func() {
			dispatchStarted <- struct{}{}
			<-releaseDispatch
		})
	}
	if _, err := bus.PublishValue(context.Background(), "telemetry", PoseSchema,
		RobotPose{X: 0, Frame: "map", Confidence: 1}); err != nil {
		t.Fatal(err)
	}
	<-dispatchStarted

	var drops []DropReason
	for i := 0; i < depth+3; i++ {
		rep, err := bus.PublishValue(context.Background(), "telemetry", PoseSchema,
			RobotPose{X: float64(i), Frame: "map", Confidence: 1})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range rep.Outcomes {
			if !o.Delivered {
				drops = append(drops, o.Reason)
			}
		}
	}
	if len(drops) != 4 {
		t.Fatalf("expected exactly 4 overflow drops (1 held at gate + %d capacity), got %d",
			depth, len(drops))
	}
	for _, r := range drops {
		if r != DropQueueFull {
			t.Fatalf("drop reason=%s want %s", r, DropQueueFull)
		}
	}
	close(releaseDispatch)
	mustWithin(t, time.Second, func() bool {
		st, _ := bus.SubscriptionStats("c1", "telemetry")
		return st.DroppedByReason[DropQueueFull] == 4
	}, "drop stats recorded")
	t.Logf("queue-full drops=%d reasons=%v stats=%+v", len(drops), drops,
		mustStats(bus, "c1", "telemetry"))
}

func TestLowLatency_OfflineDropReason(t *testing.T) {
	bus := newTestBus(t, WithQueueDepth(8))
	t.Cleanup(func() { bus.Close() })

	h := func(_ context.Context, d *Delivery) { d.Ack() }
	if _, err := bus.Subscribe("c1", "telemetry", QoSLowLatency, h); err != nil {
		t.Fatal(err)
	}
	if err := bus.Detach("c1", "telemetry"); err != nil {
		t.Fatal(err)
	}
	rep, err := bus.PublishValue(context.Background(), "telemetry", PoseSchema,
		RobotPose{Frame: "map", Confidence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Outcomes) != 1 || rep.Outcomes[0].Reason != DropSubscriberOffline {
		t.Fatalf("outcomes=%+v", rep.Outcomes)
	}
}

func TestLowLatency_RateLimitDropReason(t *testing.T) {
	bus := newTestBus(t, WithQueueDepth(1000), WithLowLatencyRateLimit(2))
	t.Cleanup(func() { bus.Close() })
	h := func(_ context.Context, d *Delivery) { d.Ack() }
	if _, err := bus.Subscribe("c1", "telemetry", QoSLowLatency, h); err != nil {
		t.Fatal(err)
	}
	sawRateLimit := false
	for i := 0; i < 20; i++ {
		rep, err := bus.PublishValue(context.Background(), "telemetry", PoseSchema,
			RobotPose{Frame: "map", Confidence: 1})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Outcomes[0].Reason == DropRateLimited {
			sawRateLimit = true
		}
	}
	if !sawRateLimit {
		t.Fatal("expected at least one rate-limited drop")
	}
}

func TestBackpressure_DoesNotBlockUnrelatedPublishers(t *testing.T) {
	bus := newTestBus(t, WithQueueDepth(2))
	t.Cleanup(func() { bus.Close() })

	// 一个永远不 Ack 的高可靠订阅者，把 topic A 填满并制造持续背压。
	block := make(chan struct{})
	staller := func(ctx context.Context, d *Delivery) {
		select {
		case <-block:
		case <-ctx.Done():
		}
	}
	if _, err := bus.Subscribe("slow", "A", QoSReliable, staller); err != nil {
		t.Fatal(err)
	}
	ctxA, cancelA := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelA()
	fillErr := make(chan error, 1)
	go func() {
		// 容量 2：1 在途 + 1 排队，第三次必然阻塞，直到 ctx 超时。
		for i := 0; i < 3; i++ {
			_, err := bus.PublishValue(ctxA, "A", PoseSchema, RobotPose{Frame: "map", Confidence: 1})
			if err != nil {
				fillErr <- err
				return
			}
		}
		fillErr <- nil
	}()

	// 另一个主题 B 必须完全不受影响：连续 20 次发布都在短时间内完成。
	ctxB, cancelB := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelB()
	got := make(chan struct{}, 20)
	fast := func(_ context.Context, d *Delivery) { d.Ack(); got <- struct{}{} }
	if _, err := bus.Subscribe("fast", "B", QoSReliable, fast); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := bus.PublishValue(ctxB, "B", PoseSchema, RobotPose{Frame: "map", Confidence: 1}); err != nil {
			t.Fatalf("unrelated publisher blocked: %v", err)
		}
	}
	for i := 0; i < 20; i++ {
		select {
		case <-got:
		case <-time.After(time.Second):
			t.Fatal("topic B subscriber stalled while A was under backpressure")
		}
	}

	err := <-fillErr
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindBackpressure {
		t.Fatalf("expected KindBackpressure on saturated topic, got %v", err)
	}
	close(block)
	t.Logf("backpressure correctly localized: saturated error=%v", err)
}

func TestReliable_BackpressureIsInterruptible(t *testing.T) {
	bus := newTestBus(t, WithQueueDepth(2))
	t.Cleanup(func() { bus.Close() })
	block := make(chan struct{})
	if _, err := bus.Subscribe("slow", "A", QoSReliable, func(ctx context.Context, d *Delivery) {
		select {
		case <-block:
		case <-ctx.Done():
		}
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := bus.PublishValue(context.Background(), "A", PoseSchema,
			RobotPose{Frame: "map", Confidence: 1}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := bus.PublishValue(ctx, "A", PoseSchema, RobotPose{Frame: "map", Confidence: 1})
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindBackpressure {
		t.Fatalf("want backpressure error, got %v", err)
	}
	close(block)
}

func mustStats(bus *Bus, client, topic string) SubStats {
	st, err := bus.SubscriptionStats(client, topic)
	if err != nil {
		return SubStats{}
	}
	return st
}

// 慢的低延迟消费者占满“在途+队列”容量后，后续发布必须立即得到 queue-full 丢弃，
// 且发布调用本身不得阻塞。
func TestLowLatency_SlowConsumerInflightOverflow(t *testing.T) {
	const depth = 4
	bus := newTestBus(t, WithQueueDepth(depth))
	t.Cleanup(func() { bus.Close() })

	gate := make(chan struct{})
	started := make(chan struct{}, depth)
	h := func(ctx context.Context, d *Delivery) {
		started <- struct{}{}
		select {
		case <-gate: // 回调阻塞：占住一个在途槽位
		case <-ctx.Done(): // 总线关闭时必须能退出，不遗留悬挂回调
		}
		d.Ack()
	}
	if _, err := bus.Subscribe("slow", "telemetry", QoSLowLatency, h); err != nil {
		t.Fatal(err)
	}

	// depth 条恰好占满：等所有回调都进入在途（每个都阻塞在 gate）。
	for i := 0; i < depth; i++ {
		if _, err := bus.PublishValue(context.Background(), "telemetry", PoseSchema,
			RobotPose{Frame: "map", Confidence: 1}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < depth; i++ {
		<-started
	}
	// 再发即溢出：同步等待应在极短时间内返回 queue-full，证明发布不被慢消费者阻塞。
	done := make(chan DropReason, 1)
	go func() {
		rep, err := bus.PublishValue(context.Background(), "telemetry", PoseSchema,
			RobotPose{Frame: "map", Confidence: 1})
		if err != nil {
			done <- DropSubscriptionClosed
			return
		}
		done <- rep.Outcomes[0].Reason
	}()
	select {
	case r := <-done:
		if r != DropQueueFull {
			t.Fatalf("reason=%v want queue-full", r)
		}
	case <-time.After(time.Second):
		t.Fatal("publisher blocked by slow low-latency consumer")
	}
	close(gate)
}
