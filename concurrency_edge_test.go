package pubsub

import (
	"context"
	"errors"
	"testing"
)

// 发布与删除订阅并发：不得 panic、不得死锁；每条结论必须可解释。
func TestConcurrent_PublishAndDelete(t *testing.T) {
	bus := newTestBus(t, WithQueueDepth(16))
	t.Cleanup(func() { bus.Close() })

	h := func(_ context.Context, d *Delivery) { d.Ack() }
	const clients = 20
	for i := 0; i < clients; i++ {
		id := clientID(i)
		if _, err := bus.Subscribe(id, "pose", QoSReliable, h); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20000; i++ {
			_, _ = bus.PublishValue(context.Background(), "pose", PoseSchema,
				RobotPose{Frame: "map", Confidence: 1})
		}
	}()
	for i := 0; i < clients; i++ {
		if err := bus.Unsubscribe(clientID(i), "pose", true); err != nil {
			t.Fatal(err)
		}
	}
	<-done
}

// QoS 升级对“之后”发布的消息立即可见：升级前的低延迟消息可被丢弃，
// 升级后的高可靠消息即使离线也必须保留并在恢复后送达。
func TestQoS_UpgradeVisibilityForLaterPublishes(t *testing.T) {
	bus := newTestBus(t, WithQueueDepth(8))
	t.Cleanup(func() { bus.Close() })

	h := func(_ context.Context, d *Delivery) { d.Ack() }
	if _, err := bus.Subscribe("c", "telemetry", QoSLowLatency, h); err != nil {
		t.Fatal(err)
	}
	if err := bus.Detach("c", "telemetry"); err != nil {
		t.Fatal(err)
	}
	// 低延迟 + 离线：允许丢弃，原因必须是 offline。
	rep, err := bus.PublishValue(context.Background(), "telemetry", PoseSchema,
		RobotPose{Frame: "map", Confidence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcomes[0].Reason != DropSubscriberOffline {
		t.Fatalf("reason=%v", rep.Outcomes[0].Reason)
	}

	if err := bus.UpdateQoS("c", "telemetry", QoSReliable); err != nil {
		t.Fatal(err)
	}
	// 升级后、仍离线：高可靠消息必须排队保留（Delivered=true 表示已被会话接收）。
	rep, err = bus.PublishValue(context.Background(), "telemetry", PoseSchema,
		RobotPose{Frame: "map", Confidence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Outcomes[0].Delivered {
		t.Fatalf("reliable msg while offline must be retained, got %+v", rep.Outcomes[0])
	}

	st, _ := bus.SubscriptionStats("c", "telemetry")
	if st.Enqueued != 1 {
		t.Fatalf("only the post-upgrade msg should be enqueued, stats=%+v", st)
	}

	got := make(chan uint64, 4)
	if _, err := bus.Subscribe("c", "telemetry", QoSReliable, func(_ context.Context, d *Delivery) {
		got <- d.Envelope.Seq
		d.Ack()
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case seq := <-got:
		if seq != 2 {
			t.Fatalf("retained msg seq=%d want 2", seq)
		}
	case <-timerC():
		t.Fatal("retained reliable message not redelivered after resume")
	}
}

// Unsubscribe 已删除会话后再次删除应返回可区分错误，不得 panic。
func TestUnsubscribe_DoubleDeleteIsDistinguishable(t *testing.T) {
	bus := newTestBus(t)
	t.Cleanup(func() { bus.Close() })
	if _, err := bus.Subscribe("c", "pose", QoSLowLatency, func(_ context.Context, d *Delivery) { d.Ack() }); err != nil {
		t.Fatal(err)
	}
	if err := bus.Unsubscribe("c", "pose", true); err != nil {
		t.Fatal(err)
	}
	err := bus.Unsubscribe("c", "pose", true)
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindInvalidArgument {
		t.Fatalf("err=%v", err)
	}
}

func clientID(i int) string {
	return "c-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	return string(b[n:])
}
