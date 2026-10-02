package pubsub_test

import (
	"context"
	"fmt"
	"log"
	"time"

	pubsub "github.com/xingjianluo/robotics-pubsub-go"
)

// poseSchema 描述 robot/pose 主题的消息版本与必填字段。
type poseSchema struct{}

func (poseSchema) TopicName() string             { return "robot/pose" }
func (poseSchema) SchemaVersion() pubsub.Version { return 1 }
func (poseSchema) RequiredFields() []string      { return []string{"x", "y"} }

func Example() {
	bus := pubsub.New(pubsub.BusConfig{
		DefaultQueueLen:          8,
		ReliableEnqueueTimeoutMs: 500,
		RedeliveryTimeoutMs:      5000,
	}, nil)
	defer bus.Close()

	bus.RegisterSchema(poseSchema{})
	// 声明该主题可接受的消息版本（编码层据此做版本兼容判定）。
	if jc, ok := bus.Codec().(*pubsub.JSONCodec); ok {
		jc.Allow("robot/pose", 1)
	}

	ctx := context.Background()
	sub, err := bus.Subscribe(ctx, "robot/pose", pubsub.QoSReliable,
		pubsub.SubscribeOption{QueueLen: 8})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("subscribed", sub.ID(), sub.Topic())

	// 高可靠发布：同主题顺序投递 + 至少一次送达。
	res, err := bus.Publish(ctx, "robot/pose", pubsub.QoSReliable,
		map[string]int{"x": 10, "y": 20})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("published seq", res.Seq, "delivered", res.Delivered)

	msg, _, err := sub.Receive(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("received seq", msg.Seq)
	if err := sub.Ack(msg.Seq); err != nil { // 确认后不会重复投递
		log.Fatal(err)
	}

	// 低延迟等级：无订阅者时得到可区分结论而不是静默失败。
	r2, _ := bus.PublishRaw(ctx, "robot/telemetry", 1,
		pubsub.QoSLowLatency, []byte(`{}`))
	if len(r2.Dropped) > 0 {
		fmt.Println("drop reason:", r2.Dropped[0].Reason)
	}

	// 短暂离线再用同一订阅者 ID 续传。
	_ = sub.Pause(ctx)
	_ = sub.Resume(ctx)

	_ = time.Millisecond
	// Output:
	// subscribed sub-1 robot/pose
	// published seq 1 delivered 1
	// received seq 1
	// drop reason: no-subscriber
}

// ExampleBus_JoinGroup 演示执行器组：同一主题下多个执行器分担任务，
// 同一机器人（Key）的消息保持顺序，掉线成员的未确认工作被转派。
func ExampleBus_JoinGroup() {
	bus := pubsub.New(pubsub.BusConfig{DefaultQueueLen: 8}, nil)
	defer bus.Close()
	ctx := context.Background()

	worker, err := bus.JoinGroup(ctx, "robot/cmd", "executors", pubsub.GroupOption{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("worker joined:", worker.ID(), "group:", worker.Group())

	// 带顺序键发布：同一 robot-7 的命令在组内严格按序执行。
	res, err := bus.PublishKeyed(ctx, "robot/cmd", "robot-7", pubsub.QoSReliable,
		map[string]string{"op": "move"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("published seq", res.Seq, "to groups:", res.GroupDelivered)

	d := <-worker.Messages()
	fmt.Println("executing seq", d.Msg.Seq, "for", d.Msg.Key)
	if err := worker.Ack(d.Msg.Seq); err != nil {
		log.Fatal(err)
	}

	// 查看死信队列（本例为空）。
	dead, _ := bus.DeadLetters("robot/cmd", "executors")
	fmt.Println("dead letters:", len(dead))

	// Output:
	// worker joined: executors/m-1 group: executors
	// published seq 1 to groups: 1
	// executing seq 1 for robot-7
	// dead letters: 0
}
