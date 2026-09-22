package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestCodec_VersionMigration(t *testing.T) {
	reg := NewRegistry()
	if err := RegisterRobotSchemas(reg); err != nil {
		t.Fatal(err)
	}

	// V1 消息：无 confidence，迁移后应有默认值 1.0，frame 缺省为 map。
	v1 := &Envelope{Topic: "t", Schema: PoseSchema, Version: PoseV1, Encoding: "json",
		Data: []byte(`{"x":1.5,"y":2.5}`)}
	v, err := reg.Decode(v1)
	if err != nil {
		t.Fatalf("v1 decode: %v", err)
	}
	p := v.(RobotPose)
	if p.X != 1.5 || p.Y != 2.5 || p.Frame != "map" || p.Confidence != 1.0 {
		t.Fatalf("unexpected migration: %+v", p)
	}

	// V2 正常消息。
	v2 := &Envelope{Schema: PoseSchema, Version: PoseV2, Encoding: "json",
		Data: []byte(`{"x":0,"y":0,"frame":"odom","confidence":0.9}`)}
	p2, err := reg.Decode(v2)
	if err != nil {
		t.Fatalf("v2 decode: %v", err)
	}
	if got := p2.(RobotPose); got.Confidence != 0.9 {
		t.Fatalf("confidence=%v", got.Confidence)
	}
}

func TestCodec_ErrorKindsDistinguishable(t *testing.T) {
	reg := NewRegistry()
	if err := RegisterRobotSchemas(reg); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		env  *Envelope
		want ErrorKind
	}{
		{
			"unknown schema",
			&Envelope{Schema: "nope", Version: 1, Encoding: "json", Data: []byte(`{}`)},
			KindUnknownSchema,
		},
		{
			"version too old",
			&Envelope{Schema: PoseSchema, Version: 0, Encoding: "json", Data: []byte(`{}`)},
			KindVersionMismatch,
		},
		{
			"version too new",
			&Envelope{Schema: PoseSchema, Version: 99, Encoding: "json", Data: []byte(`{}`)},
			KindVersionMismatch,
		},
		{
			"malformed json",
			&Envelope{Schema: PoseSchema, Version: PoseV2, Encoding: "json", Data: []byte(`{bad`)},
			KindDecode,
		},
		{
			"missing required field",
			&Envelope{Schema: PoseSchema, Version: PoseV2, Encoding: "json",
				Data: []byte(`{"x":1,"y":2,"frame":"map"}`)}, // 缺 confidence
			KindMissingField,
		},
		{
			"unsupported encoding",
			&Envelope{Schema: PoseSchema, Version: PoseV2, Encoding: "protobuf", Data: []byte(`x`)},
			KindDecode,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := reg.Decode(tc.env)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("error is not *pubsub.Error: %T %v", err, err)
			}
			if e.Kind != tc.want {
				t.Fatalf("kind=%s want=%s (err=%v)", e.Kind, tc.want, err)
			}
		})
	}
}

func TestCodec_EncodeUsesCurrentVersion(t *testing.T) {
	reg := NewRegistry()
	if err := RegisterRobotSchemas(reg); err != nil {
		t.Fatal(err)
	}
	env, err := reg.Encode(PoseSchema, RobotPose{X: 1, Y: 2, Frame: "map", Confidence: 0.8})
	if err != nil {
		t.Fatal(err)
	}
	if env.Version != PoseV2 {
		t.Fatalf("encoded version=%d want %d", env.Version, PoseV2)
	}
	var back map[string]json.RawMessage
	if err := json.Unmarshal(env.Data, &back); err != nil {
		t.Fatal(err)
	}
	if _, ok := back["confidence"]; !ok {
		t.Fatal("encoded payload missing confidence")
	}
}

func TestPublishValue_EncodeErrorMapped(t *testing.T) {
	bus := newTestBus(t)
	t.Cleanup(func() { bus.Close() })
	// 未注册 schema -> KindUnknownSchema，不返回底层错误。
	_, err := bus.PublishValue(context.Background(), "t", "no-such", struct{ X int }{1})
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindUnknownSchema {
		t.Fatalf("err=%v", err)
	}
}
