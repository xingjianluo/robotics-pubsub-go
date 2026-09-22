package pubsub

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// Schema 描述业务消息的版本信息，由调用方的消息类型实现。
type Schema interface {
	TopicName() string
	SchemaVersion() Version
	RequiredFields() []string
}

// Codec 负责业务负载的编解码与版本兼容判定。
type Codec interface {
	// Encode 将 v 编码为主题 s 的当前版本字节。
	Encode(ctx context.Context, s Schema, v any) ([]byte, error)
	// Decode 将 data 按 s 声明的版本解码；版本不兼容 / 字段缺失 /
	// 编码异常分别映射为 KindVersionMismatch / KindMissingField / KindDecode。
	Decode(ctx context.Context, s Schema, data []byte) (any, error)
}

// ---------- JSON Codec ----------

// JSONCodec 把消息版本写入 JSON 的 "_v" 字段，
// 并在解码后校验版本与必填字段。
type JSONCodec struct {
	// AcceptVersions 声明每个主题可接受的版本集合。
	AcceptVersions map[string]map[Version]bool
}

// NewJSONCodec 创建 JSON 编解码器。
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{AcceptVersions: map[string]map[Version]bool{}}
}

// Allow 注册主题可接受的版本（至少注册一个）。
func (c *JSONCodec) Allow(topic string, versions ...Version) *JSONCodec {
	set := c.AcceptVersions[topic]
	if set == nil {
		set = map[Version]bool{}
		c.AcceptVersions[topic] = set
	}
	for _, v := range versions {
		set[v] = true
	}
	return c
}

type versionedBody struct {
	V       Version         `json:"_v"`
	Payload json.RawMessage `json:"p"`
}

func (c *JSONCodec) Encode(_ context.Context, s Schema, v any) ([]byte, error) {
	topic := ""
	if s != nil {
		topic = s.TopicName()
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, newError(KindDecode, topic, "json marshal failed", err)
	}
	out, err := json.Marshal(&versionedBody{V: s.SchemaVersion(), Payload: raw})
	if err != nil {
		return nil, newError(KindDecode, topic, "wrap envelope failed", err)
	}
	return out, nil
}

func (c *JSONCodec) Decode(_ context.Context, s Schema, data []byte) (any, error) {
	topic := s.TopicName()
	var vb versionedBody
	if err := json.Unmarshal(data, &vb); err != nil {
		return nil, newError(KindDecode, topic, "payload is not valid json", err)
	}
	if set, ok := c.AcceptVersions[topic]; ok && len(set) > 0 && !set[vb.V] {
		return nil, newError(KindVersionMismatch, topic,
			fmt.Sprintf("version %d not accepted (want one of %v)", vb.V, acceptedList(set)), nil)
	}
	// 字段缺失检查（在解码后的 map 上做）。
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(vb.Payload, &fields); err != nil {
		return nil, newError(KindDecode, topic, "payload body is not a json object", err)
	}
	for _, f := range s.RequiredFields() {
		raw, ok := fields[f]
		if !ok || isNull(raw) {
			return nil, newError(KindMissingField, topic,
				"required field missing: "+f, nil)
		}
	}
	// 返回原始业务字节，调用方可按需再反序列化。
	return []byte(vb.Payload), nil
}

func isNull(raw json.RawMessage) bool {
	return len(bytes.TrimSpace(raw)) == 0 ||
		(len(raw) == 4 && raw[0] == 'n' && raw[1] == 'u' && raw[2] == 'l' && raw[3] == 'l')
}

func acceptedList(set map[Version]bool) []Version {
	out := make([]Version, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	return out
}

// ---------- 二进制帧 Codec（Envelope 封包 / 拆包） ----------

// 帧布局（大端）：
//
//	magic[4]="RPS1" | version uint16 | seq uint64
//	topicLen uint16 | idLen uint16 | topic | id | body
type frameCodec struct{}

func newFrameCodec() *frameCodec { return &frameCodec{} }

var frameMagic = [4]byte{'R', 'P', 'S', '1'}

func (f *frameCodec) Marshal(env *Envelope) ([]byte, error) {
	if env == nil {
		return nil, newError(KindInvalidArgument, "", "nil envelope", nil)
	}
	if len(env.Topic) > 65535 || len(env.ID) > 65535 {
		return nil, newError(KindInvalidArgument, env.Topic, "topic/id too long", nil)
	}
	buf := make([]byte, 0, 4+2+8+2+2+len(env.Topic)+len(env.ID)+len(env.Body))
	out := bytes.NewBuffer(buf)
	_ = out.WriteByte(frameMagic[0])
	_ = out.WriteByte(frameMagic[1])
	_ = out.WriteByte(frameMagic[2])
	_ = out.WriteByte(frameMagic[3])
	var hdr [10]byte
	binary.BigEndian.PutUint16(hdr[0:2], uint16(env.Version))
	binary.BigEndian.PutUint64(hdr[2:10], env.Seq)
	_, _ = out.Write(hdr[:])
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(env.Topic)))
	_, _ = out.Write(l[:])
	binary.BigEndian.PutUint16(l[:], uint16(len(env.ID)))
	_, _ = out.Write(l[:])
	_, _ = out.WriteString(env.Topic)
	_, _ = out.WriteString(env.ID)
	_, _ = out.Write(env.Body)
	return out.Bytes(), nil
}

func (f *frameCodec) Unmarshal(data []byte) (*Envelope, error) {
	if len(data) < 4+2+8+2+2 {
		return nil, newError(KindDecode, "", "frame too short", nil)
	}
	if data[0] != frameMagic[0] || data[1] != frameMagic[1] ||
		data[2] != frameMagic[2] || data[3] != frameMagic[3] {
		return nil, newError(KindDecode, "", "bad magic", nil)
	}
	p := 4
	ver := Version(binary.BigEndian.Uint16(data[p:]))
	seq := binary.BigEndian.Uint64(data[p+2:])
	p += 10
	topicLen := int(binary.BigEndian.Uint16(data[p:]))
	idLen := int(binary.BigEndian.Uint16(data[p+2:]))
	p += 4
	if len(data) < p+topicLen+idLen {
		return nil, newError(KindDecode, "", "frame truncated", nil)
	}
	topic := string(data[p : p+topicLen])
	p += topicLen
	id := string(data[p : p+idLen])
	p += idLen
	body := data[p:]
	return &Envelope{ID: id, Topic: topic, Version: ver, Seq: seq, Body: body}, nil
}
