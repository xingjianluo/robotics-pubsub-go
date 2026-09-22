package pubsub

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentPublish_ReliableOrderUnderRace(t *testing.T) {
	const publishers = 8
	const perPublisher = 200
	bus := newTestBus(t, WithQueueDepth(512))
	t.Cleanup(func() { bus.Close() })

	var mu sync.Mutex
	seqs := []uint64{}
	h := func(_ context.Context, d *Delivery) {
		mu.Lock()
		seqs = append(seqs, d.Envelope.Seq)
		mu.Unlock()
		d.Ack()
	}
	if _, err := bus.Subscribe("robot-x", "pose", QoSReliable, h); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				if _, err := bus.PublishValue(context.Background(), "pose", PoseSchema,
					RobotPose{Frame: "map", Confidence: 1}); err != nil {
					t.Errorf("publish: %v", returnErr(err))
					return
				}
			}
		}()
	}
	wg.Wait()

	total := publishers * perPublisher
	mustWithin(t, 5*time.Second, func() bool {
		mu.Lock()
		n := len(seqs)
		mu.Unlock()
		return n == total
	}, fmt.Sprintf("all %d messages delivered", total))

	mu.Lock()
	defer mu.Unlock()
	if len(seqs) != total {
		t.Fatalf("delivered=%d want=%d", len(seqs), total)
	}
	seen := make(map[uint64]int, total)
	for i, s := range seqs {
		if s != uint64(i+1) {
			t.Fatalf("order broken at index %d: seq=%d", i, s)
		}
		seen[s]++
	}
	for s, n := range seen {
		if n != 1 {
			t.Fatalf("seq=%d delivered %d times", s, n)
		}
	}
}

func TestConcurrent_ReconnectChurnNoLoss(t *testing.T) {
	bus := newTestBus(t, WithQueueDepth(512))
	t.Cleanup(func() { bus.Close() })

	var mu sync.Mutex
	delivered := map[uint64]int{}
	var maxSeq uint64
	handler := func(_ context.Context, d *Delivery) {
		mu.Lock()
		delivered[d.Envelope.Seq]++
		if d.Envelope.Seq > maxSeq {
			maxSeq = d.Envelope.Seq
		}
		mu.Unlock()
		d.Ack()
	}
	if _, err := bus.Subscribe("churn", "pose", QoSReliable, handler); err != nil {
		t.Fatal(err)
	}

	var done atomic.Bool
	var pubWG sync.WaitGroup
	pubWG.Add(1)
	go func() { // 持续发布
		defer pubWG.Done()
		for !done.Load() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			if _, err := bus.PublishValue(ctx, "pose", PoseSchema,
				RobotPose{Frame: "map", Confidence: 1}); err != nil {
				cancel()
				return
			}
			cancel()
		}
	}()

	// 反复 detach / 用相同 clientID 恢复。
	for i := 0; i < 20; i++ {
		if err := bus.Detach("churn", "pose"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
		if _, err := bus.Subscribe("churn", "pose", QoSReliable, handler); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}

	done.Store(true)
	pubWG.Wait()

	// 停止发布后再做一次重连，确保所有在途/离线期间消息全部结算。
	if err := bus.Detach("churn", "pose"); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Subscribe("churn", "pose", QoSReliable, handler); err != nil {
		t.Fatal(err)
	}

	mustWithin(t, 5*time.Second, func() bool {
		st, _ := bus.SubscriptionStats("churn", "pose")
		return st.CurrentQueueLength == 0
	}, "queue drains after final resume")

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != int(maxSeq) {
		// 找出丢失的序号，保证失败可复现、原因可区分。
		var missing []uint64
		for s := uint64(1); s <= maxSeq; s++ {
			if delivered[s] == 0 {
				missing = append(missing, s)
			}
		}
		t.Fatalf("loss detected: maxSeq=%d unique=%d missing(first10)=%v",
			maxSeq, len(delivered), firstN(missing, 10))
	}
	// 至少一次允许断连导致的重投；校验重投账目自洽：每消息(投递次数-1)之和
	// 必须等于总线统计的 Redelivered，且任何消息都不会在确认后再次投递。
	var extra int64
	for _, n := range delivered {
		if n < 1 {
			t.Fatal("unreachable: zero count in map")
		}
		extra += int64(n - 1)
	}
	st, _ := bus.SubscriptionStats("churn", "pose")
	if extra != st.Redelivered {
		t.Fatalf("redelivery accounting mismatch: sum(extra)=%d stats.Redelivered=%d",
			extra, st.Redelivered)
	}
	t.Logf("churn OK: maxSeq=%d uniqueDelivered=%d redeliveries=%d",
		maxSeq, len(delivered), extra)
}

func TestConcurrent_SubscriptionVisibilityImmediate(t *testing.T) {
	bus := newTestBus(t, WithQueueDepth(64))
	t.Cleanup(func() { bus.Close() })
	h := func(_ context.Context, d *Delivery) { d.Ack() }

	if _, err := bus.Subscribe("a", "pose", QoSReliable, h); err != nil {
		t.Fatal(err)
	}
	mustWithin(t, time.Second, func() bool {
		rep, err := bus.PublishValue(context.Background(), "pose", PoseSchema,
			RobotPose{Frame: "map", Confidence: 1})
		return err == nil && len(rep.Outcomes) == 1
	}, "one recipient before join")

	if _, err := bus.Subscribe("b", "pose", QoSReliable, h); err != nil {
		t.Fatal(err)
	}
	rep, err := bus.PublishValue(context.Background(), "pose", PoseSchema,
		RobotPose{Frame: "map", Confidence: 1})
	if err != nil || len(rep.Outcomes) != 2 {
		t.Fatalf("after join recipients=%d err=%v", len(rep.Outcomes), err)
	}
	if err := bus.Unsubscribe("a", "pose", true); err != nil {
		t.Fatal(err)
	}
	rep, err = bus.PublishValue(context.Background(), "pose", PoseSchema,
		RobotPose{Frame: "map", Confidence: 1})
	if err != nil || len(rep.Outcomes) != 1 || rep.Outcomes[0].ClientID != "b" {
		t.Fatalf("after delete recipients=%+v err=%v", rep.Outcomes, err)
	}
}

func firstN(s []uint64, n int) []uint64 {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func returnErr(err error) error { return err }
