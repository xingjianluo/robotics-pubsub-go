package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestResumeUnderLoadNoLoss 一边压测一边反复离线/续传（老路径回归）：
// 任何时候都不丢消息、首投严格按序号升序、不偶发超时。
//
// 场景覆盖：持续可靠发布（背压重试）+ 消费者即收即确认 +
// 高频 Pause/Resume 搅动（续传窗口与在途发送重叠）。
//
// 判定依据：
//   - 不丢：最终必须收齐 1..total 每一条（收不齐则超时判负）；
//   - 不乱：每条消息的“首次出现”必须严格大于上一条首次出现；
//   - 不重：已确认消息只允许以“续传窗口滞留副本”形式偶发出现，
//     计数容忍（at-least-once），但绝不允许影响收齐判定。
func TestResumeUnderLoadNoLoss(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 8, ReliableEnqueueTimeoutMs: 5000,
		RedeliveryTimeoutMs: 60000}, nil)
	defer bus.Close()
	ctx := context.Background()
	sub, err := bus.Subscribe(ctx, "t/resume", QoSReliable, SubscribeOption{QueueLen: 8})
	if err != nil {
		t.Fatal(err)
	}

	const total = 400

	// 测试提前判负时通知后台协程退出，并保证它们先于 bus.Close 停下。
	done := make(chan struct{})
	var pubWg, churnWg sync.WaitGroup
	defer func() {
		close(done)
		pubWg.Wait()
		churnWg.Wait()
	}()

	// 发布侧：持续可靠发布，背压超限则让出重试（不丢任何一条）。
	pubWg.Add(1)
	go func() {
		defer pubWg.Done()
		for i := 0; i < total; i++ {
			for {
				select {
				case <-done:
					return
				default:
				}
				if _, err := bus.PublishRaw(ctx, "t/resume", 1, QoSReliable, []byte{byte(i)}); err == nil {
					break
				} else if IsError(err, KindOverflow) {
					time.Sleep(time.Millisecond)
					continue
				} else {
					select {
					case <-done:
					default:
						t.Errorf("publish %d: %v", i, err)
					}
					return
				}
			}
		}
	}()

	// 搅动侧：高频离线/续传，让 resetForResume 与在途发送反复重叠。
	churnWg.Add(1)
	go func() {
		defer churnWg.Done()
		for i := 0; i < 60; i++ {
			select {
			case <-done:
				return
			default:
			}
			if err := sub.Pause(ctx); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
			if err := sub.Resume(ctx); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// 消费侧：即收即确认；通道因续传重建时重新获取。
	received := make([]bool, total+1)
	var firstLast uint64
	var stale, got int
	deadline := time.After(20 * time.Second)
	for got < total {
		select {
		case d, ok := <-sub.Messages():
			if !ok {
				continue // 续传重建通道，下一轮重新获取
			}
			seq := d.Msg.Seq
			if seq < 1 || seq > total {
				t.Fatalf("unexpected seq=%d", seq)
			}
			if received[seq] {
				stale++ // 续传窗口滞留副本：容忍，仅计数
				continue
			}
			if seq <= firstLast {
				t.Fatalf("first-delivery order broken: seq=%d after %d", seq, firstLast)
			}
			firstLast = seq
			received[seq] = true
			got++
			if err := sub.Ack(seq); err != nil {
				t.Fatalf("ack seq=%d: %v", seq, err)
			}
		case <-deadline:
			t.Fatalf("timeout: received %d/%d (stale=%d) — message lost or dispatcher stuck after resume",
				got, total, stale)
		}
	}
	pubWg.Wait()
	churnWg.Wait()
	t.Logf("resume-under-load ok: %d/%d received, first-deliveries strictly ordered, stale-copies=%d",
		got, total, stale)
}
