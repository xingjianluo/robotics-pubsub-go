package pubsub

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordingHandler 收到即按序 Ack，记录收到的 seq 与投递尝试次数。
type recordingHandler struct {
	mu       sync.Mutex
	seqs     []uint64
	attempts map[uint64]int
}

func newRecording() *recordingHandler {
	return &recordingHandler{attempts: map[uint64]int{}}
}

func (r *recordingHandler) handle(_ context.Context, d *Delivery) {
	r.mu.Lock()
	r.seqs = append(r.seqs, d.Envelope.Seq)
	r.attempts[d.Envelope.Seq] = d.Attempt
	r.mu.Unlock()
	d.Ack()
}

func (r *recordingHandler) snapshot() ([]uint64, map[uint64]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]uint64(nil), r.seqs...)
	att := make(map[uint64]int, len(r.attempts))
	for k, v := range r.attempts {
		att[k] = v
	}
	return out, att
}

func TestReliable_OrderedDeliveryAndAckOnce(t *testing.T) {
	bus := newTestBus(t, WithQueueDepth(64))
	t.Cleanup(func() { bus.Close() })

	h := newRecording()
	if _, err := bus.Subscribe("robot-1", "pose", QoSReliable, h.handle); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if _, err := bus.PublishValue(ctx, "pose", PoseSchema,
			RobotPose{X: float64(i), Y: 1, Frame: "map", Confidence: 1}); err != nil {
			t.Fatal(err)
		}
	}
	mustWithin(t, time.Second, func() bool {
		st, _ := bus.SubscriptionStats("robot-1", "pose")
		return st.Acked == 50 && st.CurrentQueueLength == 0
	}, "all 50 acked and queue drained")

	seqs, att := h.snapshot()
	for i, s := range seqs {
		if s != uint64(i+1) {
			t.Fatalf("order broken at %d: got seq %d", i, s)
		}
		if att[s] != 1 {
			t.Fatalf("seq=%d delivered %d times, want exactly once", s, att[s])
		}
	}
	stats, _ := bus.SubscriptionStats("robot-1", "pose")
	if stats.Acked != 50 || stats.Redelivered != 0 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestReliable_NackTriggersInOrderRedelivery(t *testing.T) {
	bus := newTestBus(t, WithQueueDepth(64))
	t.Cleanup(func() { bus.Close() })

	var nacks atomic.Int64
	var mu sync.Mutex
	var order []uint64
	h := func(_ context.Context, d *Delivery) {
		mu.Lock()
		order = append(order, d.Envelope.Seq)
		mu.Unlock()
		if d.Envelope.Seq == 2 && nacks.Add(1) == 1 {
			d.Nack() // 首次拒绝；seq3 必须等 seq2 重投确认后才派发
			return
		}
		d.Ack()
	}
	if _, err := bus.Subscribe("robot-1", "pose", QoSReliable, h); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := bus.PublishValue(context.Background(), "pose", PoseSchema,
			RobotPose{X: float64(i), Y: 0, Frame: "map", Confidence: 1}); err != nil {
			t.Fatal(err)
		}
	}
	mustWithin(t, time.Second, func() bool {
		st, _ := bus.SubscriptionStats("robot-1", "pose")
		return st.Acked == 4
	}, "all acked after nack retry")

	mu.Lock()
	defer mu.Unlock()
	want := []uint64{1, 2, 2, 3, 4} // 重投严格排在 3、4 之前
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("order=%v want=%v", order, want)
	}
}

func TestReliable_SessionResumeNoLossNoDuplicate(t *testing.T) {
	bus := newTestBus(t, WithQueueDepth(128))
	t.Cleanup(func() { bus.Close() })

	var mu sync.Mutex
	counts := map[uint64]int{}
	order := []uint64{}
	seq2InHandler := make(chan struct{})

	// 阶段一（在线）：seq1 确认；seq2 进入 handler 后阻塞，随后会话被 detach；
	// seq3 在离线期间发布。重连后期望：seq1 不重复，seq2 重投一次，seq3 送达。
	h1 := func(ctx context.Context, d *Delivery) {
		switch d.Envelope.Seq {
		case 1:
			mu.Lock()
			counts[1]++
			order = append(order, 1)
			mu.Unlock()
			d.Ack()
		case 2:
			close(seq2InHandler)
			<-ctx.Done() // 连接失效后旧 handler 退出，不做裁决
		default:
			<-ctx.Done()
		}
	}
	if _, err := bus.Subscribe("robot-9", "pose", QoSReliable, h1); err != nil {
		t.Fatal(err)
	}
	pub := func() {
		if _, err := bus.PublishValue(context.Background(), "pose", PoseSchema,
			RobotPose{Frame: "map", Confidence: 1}); err != nil {
			t.Error(err)
		}
	}
	pub() // seq1
	pub() // seq2
	<-seq2InHandler

	epochBefore := bus.TopicEpoch("pose")
	if err := bus.Detach("robot-9", "pose"); err != nil {
		t.Fatal(err)
	}
	mustWithin(t, time.Second, func() bool {
		return bus.TopicEpoch("pose") == epochBefore+1
	}, "epoch advances on detach (visibility)")

	// 离线期间发布的高可靠消息必须保留，静默丢失即失败。
	pub() // seq3

	h2 := newRecording()
	if _, err := bus.Subscribe("robot-9", "pose", QoSReliable, h2.handle); err != nil {
		t.Fatal(err)
	}
	mustWithin(t, 2*time.Second, func() bool {
		_, att := h2.snapshot()
		return att[2] >= 1 && att[3] == 1
	}, "seq2 redelivered and seq3 delivered after resume")

	// 关键不变量：seq1 已确认，重连后不得再投。
	mu.Lock()
	if counts[1] != 1 {
		t.Fatalf("seq1 delivered %d times, acked msg must not duplicate", counts[1])
	}
	mu.Unlock()

	seqs2, att2 := h2.snapshot()
	want := []uint64{2, 3}
	if fmt.Sprint(seqs2) != fmt.Sprint(want) {
		t.Fatalf("post-resume order=%v want=%v", seqs2, want)
	}
	if att2[2] != 2 { // 重连后的这一次投递 attempt=2，redelivered=true
		t.Fatalf("seq2 post-resume attempt=%d want 2", att2[2])
	}
	stats, _ := bus.SubscriptionStats("robot-9", "pose")
	if stats.Redelivered < 1 {
		t.Fatalf("expected redelivery stat>=1, got %+v", stats)
	}
	t.Logf("resume verified: counts=%v stats=%+v", counts, stats)
}
