package pubsub

import (
	"context"
	"testing"
)

type poseV2 struct{}

func (poseV2) TopicName() string        { return "robot/pose" }
func (poseV2) SchemaVersion() Version   { return 2 }
func (poseV2) RequiredFields() []string { return []string{"x", "y"} }

func TestCodecRoundTrip(t *testing.T) {
	jc := NewJSONCodec().Allow("robot/pose", 1, 2)
	ctx := context.Background()
	raw, err := jc.Encode(ctx, poseV2{}, map[string]int{"x": 1, "y": 2})
	if err != nil {
		t.Fatal(err)
	}
	v, err := jc.Decode(ctx, poseV2{}, raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	t.Logf("round-trip payload=%s", v)
}

func TestCodecVersionMismatch(t *testing.T) {
	jc := NewJSONCodec().Allow("robot/pose", 1) // 只接受 v1
	ctx := context.Background()
	raw, _ := jc.Encode(ctx, poseV2{}, map[string]int{"x": 1, "y": 2}) // 产生 v2
	_, err := jc.Decode(ctx, poseV2{}, raw)
	if err == nil {
		t.Fatal("expected version mismatch, got nil")
	}
	if !IsError(err, KindVersionMismatch) {
		t.Fatalf("expected KindVersionMismatch, got %v", err)
	}
	t.Logf("version mismatch mapped: %v", err)
}

func TestCodecMissingField(t *testing.T) {
	jc := NewJSONCodec().Allow("robot/pose", 2)
	ctx := context.Background()
	raw, _ := jc.Encode(ctx, poseV2{}, map[string]int{"x": 1}) // 缺 y
	_, err := jc.Decode(ctx, poseV2{}, raw)
	if !IsError(err, KindMissingField) {
		t.Fatalf("expected KindMissingField, got %v", err)
	}
	t.Logf("missing field mapped: %v", err)
}

func TestCodecDecodeGarbage(t *testing.T) {
	jc := NewJSONCodec().Allow("robot/pose", 2)
	_, err := jc.Decode(context.Background(), poseV2{}, []byte("not-json"))
	if !IsError(err, KindDecode) {
		t.Fatalf("expected KindDecode, got %v", err)
	}
	t.Logf("garbage mapped: %v", err)
}

func TestFrameMarshalRoundTrip(t *testing.T) {
	f := newFrameCodec()
	env := &Envelope{ID: "abc", Topic: "robot/cmd", Version: 3, Seq: 42, Body: []byte{1, 2, 3}}
	data, err := f.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != env.ID || got.Topic != env.Topic || got.Version != env.Version ||
		got.Seq != env.Seq || string(got.Body) != string(env.Body) {
		t.Fatalf("frame mismatch: %+v", got)
	}
	if _, err := f.Unmarshal([]byte{0, 1, 2}); err == nil || !IsError(err, KindDecode) {
		t.Fatalf("expected KindDecode on short frame, got %v", err)
	}
	t.Logf("frame round-trip ok: id=%s seq=%d ver=%d", got.ID, got.Seq, got.Version)
}

// TestReceiveDecodeErrors 通过总线发布“畸形/错版本”字节，验证订阅侧
// 解码错误 Kind 可区分，且底层异常不透出。
func TestReceiveDecodeErrors(t *testing.T) {
	bus := New(BusConfig{DefaultQueueLen: 8}, nil)
	defer bus.Close()
	bus.RegisterSchema(poseV2{})
	if jc, ok := bus.codec.(*JSONCodec); ok {
		jc.Allow("robot/pose", 2)
	}
	ctx := context.Background()
	sub, _ := bus.Subscribe(ctx, "robot/pose", QoSReliable, SubscribeOption{QueueLen: 8})

	// 直接塞入一个错版本的“已编码”体：_v=9
	if _, err := bus.PublishRaw(ctx, "robot/pose", 9, QoSReliable,
		[]byte(`{"_v":9,"p":{"x":1,"y":2}}`)); err != nil {
		t.Fatal(err)
	}
	msg, _, err := sub.Receive(ctx)
	if err == nil {
		t.Fatal("expected decode error")
	}
	if !IsError(err, KindVersionMismatch) {
		t.Fatalf("expected KindVersionMismatch got %v", err)
	}
	t.Logf("seq=%d rejected: %v", msg.Seq, err)
}
