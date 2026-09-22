package pubsub

import (
	"context"
	"testing"
	"time"
)

type schemaA struct{}

func (schemaA) TopicName() string        { return "robot/pose" }
func (schemaA) SchemaVersion() Version   { return 2 }
func (schemaA) RequiredFields() []string { return []string{"x", "y"} }

func TestSmokeBasic(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 8, ReliableEnqueueTimeoutMs: 500, RedeliveryTimeoutMs: 5000}, nil)
	defer bus.Close()
	bus.RegisterSchema(schemaA{})
	if jc, ok := bus.codec.(*JSONCodec); ok {
		jc.Allow("robot/pose", 2)
	}
	ctx := context.Background()
	sub, err := bus.Subscribe(ctx, "robot/pose", QoSReliable, SubscribeOption{QueueLen: 8})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Publish(ctx, "robot/pose", QoSReliable, map[string]int{"x": 1, "y": 2}); err != nil {
		t.Fatal(err)
	}
	msg, val, err := receiveWithTimeout(sub, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("got seq=%d id=%s ver=%d payload=%s", msg.Seq, msg.ID, msg.Version, val)
	if err := sub.Ack(msg.Seq); err != nil {
		t.Fatal(err)
	}
}

func receiveWithTimeout(s *Subscription, d time.Duration) (*Message, any, error) {
	type res struct {
		m   *Message
		v   any
		err error
	}
	ch := make(chan res, 1)
	go func() {
		m, v, err := s.Receive(context.Background())
		ch <- res{m, v, err}
	}()
	select {
	case r := <-ch:
		return r.m, r.v, r.err
	case <-time.After(d):
		return nil, nil, context.DeadlineExceeded
	}
}

func TestSmokeResume(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 64, ReliableEnqueueTimeoutMs: 500, RedeliveryTimeoutMs: 60000}, nil)
	defer bus.Close()
	ctx := context.Background()
	sub, err := bus.Subscribe(ctx, "robot/cmd", QoSReliable, SubscribeOption{QueueLen: 64})
	if err != nil {
		t.Fatal(err)
	}

	// 先发 3 条：收到前 2 条并 Ack；第 3 条不 Ack。
	for i := 0; i < 3; i++ {
		if _, err := bus.PublishRaw(ctx, "robot/cmd", 1, QoSReliable, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	m1, err2 := recv(sub, time.Second)
	if err2 != nil {
		t.Fatal(err2)
	}
	m2, err2 := recv(sub, time.Second)
	if err2 != nil {
		t.Fatal(err2)
	}
	if m1.Seq != 1 || m2.Seq != 2 {
		t.Fatalf("expected seq 1,2 got %d,%d", m1.Seq, m2.Seq)
	}
	if err := sub.Ack(1); err != nil {
		t.Fatal(err)
	}
	if err := sub.Ack(2); err != nil {
		t.Fatal(err)
	}
	m3, err2 := recv(sub, time.Second)
	if err2 != nil {
		t.Fatal(err2)
	}
	if m3.Seq != 3 {
		t.Fatalf("expected seq 3 got %d", m3.Seq)
	}

	// 订阅者短暂离线，期间继续发布 2 条。
	if err := sub.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := bus.PublishRaw(ctx, "robot/cmd", 1, QoSReliable, []byte{9}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sub.Resume(ctx); err != nil {
		t.Fatal(err)
	}

	// resume 后必须收到 3,4,5（3 未 Ack 重投，4,5 续传），且 1,2 不重复。
	want := []uint64{3, 4, 5}
	for _, w := range want {
		m, err := recv(sub, 2*time.Second)
		if err != nil {
			t.Fatalf("waiting seq %d: %v", w, err)
		}
		t.Logf("resume delivery seq=%d attempt-check", m.Seq)
		if m.Seq != w {
			t.Fatalf("expected seq %d got %d", w, m.Seq)
		}
		if err := sub.Ack(m.Seq); err != nil {
			t.Fatal(err)
		}
	}
}

func recv(s *Subscription, d time.Duration) (*Message, error) {
	m, _, err := receiveWithTimeout(s, d)
	return m, err
}
