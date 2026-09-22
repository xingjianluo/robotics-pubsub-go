// Command demo 在单进程内模拟多个发布者与订阅者，端到端演示：
//   - 高可靠等级（robot/pose）：顺序投递、至少一次、离线重连续传、Ack 去重
//   - 低延迟等级（robot/telemetry）：尽力投递，溢出/离线给出可区分丢弃原因
//   - 消息编码与版本兼容：V1 旧消息迁移、V2 新消息解析、非法版本明确拒绝
//
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	pubsub "github.com/xingjianluo/robotics-pubsub-go"
)

func main() {
	reg := pubsub.NewRegistry()
	if err := pubsub.RegisterRobotSchemas(reg); err != nil {
		fail(err)
	}

	bus := pubsub.New(reg,
		pubsub.WithQueueDepth(8),
		pubsub.WithDropPolicy(pubsub.DropNewest),
	)
	defer bus.Close()
	ctx := context.Background()

	// --- 高可靠订阅者 R：记录顺序，前两条确认，第三条先不确认模拟处理中离线 ---
	poseSeen := make(chan uint64, 16)
	seq3InHandler := make(chan struct{})
	rHandler := func(_ context.Context, d *pubsub.Delivery) {
		poseSeen <- d.Envelope.Seq
		switch d.Envelope.Seq {
		case 1, 2:
			d.Ack()
		case 3:
			close(seq3InHandler)
			time.Sleep(400 * time.Millisecond) // 模拟处理期间网络中断
			d.Ack()
		default:
			d.Ack()
		}
	}
	if _, err := bus.Subscribe("robot-r", "robot/pose", pubsub.QoSReliable, rHandler); err != nil {
		fail(err)
	}

	// --- 低延迟订阅者 L：前若干个回调受控阻塞，占满“在途+队列”容量，
	// 从而确定性演示队列溢出；之后释放，观察新消息恢复投递。 ---
	llGate := make(chan struct{})
	var llStarted atomic.Int32
	llDone := make(chan uint64, 64)
	if _, err := bus.Subscribe("robot-l", "robot/telemetry", pubsub.QoSLowLatency,
		func(_ context.Context, d *pubsub.Delivery) {
			if llStarted.Add(1) <= 8 { // 在途+队列容量（深度 8）全部被占住
				<-llGate
			}
			if v, err := reg.Decode(d.Envelope); err != nil {
				fmt.Printf("  [L] decode rejected seq=%d: %v\n", d.Envelope.Seq, err)
			} else if p, ok := v.(pubsub.RobotPose); ok {
				_ = p
			}
			d.Ack()
			llDone <- d.Envelope.Seq
		}); err != nil {
		fail(err)
	}

	fmt.Println("== 1) 高可靠：顺序发布 seq1..seq3 ==")
	for i := 0; i < 3; i++ {
		if _, err := bus.PublishValue(ctx, "robot/pose", pubsub.PoseSchema,
			pubsub.RobotPose{X: float64(i), Y: 0, Frame: "map", Confidence: 0.99}); err != nil {
			fail(err)
		}
	}
	<-seq3InHandler
	fmt.Println("== 2) seq3 在途时订阅者短暂离线（Detach），期间再发 seq4 ==")
	if err := bus.Detach("robot-r", "robot/pose"); err != nil {
		fail(err)
	}
	if _, err := bus.PublishValue(ctx, "robot/pose", pubsub.PoseSchema,
		pubsub.RobotPose{X: 3, Frame: "map", Confidence: 0.99}); err != nil {
		fail(err)
	}
	time.Sleep(100 * time.Millisecond)
	fmt.Println("== 3) 相同 clientID 重新接入：seq1/2 不重投，seq3/4 按序结算 ==")
	if _, err := bus.Subscribe("robot-r", "robot/pose", pubsub.QoSReliable, func(c context.Context, d *pubsub.Delivery) {
		poseSeen <- d.Envelope.Seq
		fmt.Printf("  [R-resumed] seq=%d redelivered=%v\n", d.Envelope.Seq, d.Redelivered)
		d.Ack()
	}); err != nil {
		fail(err)
	}

	fmt.Println("== 4) 低延迟：慢消费者占满容量后突发 12 条，观察可区分丢弃原因 ==")
	// 先投递 8 条填满“在途+队列”容量（回调均阻塞在 llGate）。
	for i := 0; i < 8; i++ {
		if _, err := bus.PublishValue(ctx, "robot/telemetry", pubsub.PoseSchema,
			pubsub.RobotPose{X: float64(i), Frame: "odom", Confidence: 0.5}); err != nil {
			fail(err)
		}
	}
	time.Sleep(50 * time.Millisecond) // 等 8 个回调全部进入在途

	dropped := map[pubsub.DropReason]int{}
	for i := 0; i < 12; i++ {
		rep, err := bus.PublishValue(ctx, "robot/telemetry", pubsub.PoseSchema,
			pubsub.RobotPose{X: float64(100 + i), Frame: "odom", Confidence: 0.5})
		if err != nil {
			fail(err)
		}
		for _, o := range rep.Outcomes {
			if !o.Delivered {
				dropped[o.Reason]++
			}
		}
	}
	fmt.Printf("  溢出期间丢弃统计（原因可区分）：%v（发布全程未阻塞）\n", dropped)
	close(llGate) // 释放慢消费者，在途消息恢复处理，后续容量恢复

	fmt.Println("== 5) 版本兼容：V1 可迁移解析；过新版本被明确拒绝 ==")
	if v, err := reg.Decode(&pubsub.Envelope{
		Schema: pubsub.PoseSchema, Version: pubsub.PoseV1, Encoding: "json",
		Data: []byte(`{"x":1.0,"y":2.0}`),
	}); err != nil {
		fail(err)
	} else {
		fmt.Printf("  V1 迁移结果：%+v（confidence 补默认值）\n", v)
	}
	if _, err := reg.Decode(&pubsub.Envelope{
		Schema: pubsub.PoseSchema, Version: 99, Encoding: "json", Data: []byte(`{}`),
	}); err != nil {
		fmt.Printf("  不兼容版本被拒绝（错误类型可区分）：%v\n", err)
	}

	// 等待重连后的消息结算并核对投递序列
	time.Sleep(300 * time.Millisecond)
	close(poseSeen)
	var seq []uint64
	for s := range poseSeen {
		seq = append(seq, s)
	}
	fmt.Printf("== 结论：R 实际看到的 seq 序列 = %v（1、2 仅一次；3、4 不丢）==\n", seq)

	st, err := bus.SubscriptionStats("robot-r", "robot/pose")
	if err != nil {
		fail(err)
	}
	fmt.Printf("== R 会话判定计数：%+v ==\n", st)

	if err := bus.Close(); err != nil {
		fail(err)
	}
	fmt.Println("demo finished")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "demo error:", err)
	os.Exit(1)
}
