package pubsub

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// SchemaRegistry 保存 schema 与其支持的版本范围，负责版本兼容与错误映射。
// 每个 schema 注册一个解码函数：它收到原始信封后自行完成
// “旧版本字段 -> 当前版本结构”的迁移，或返回明确的拒绝错误。
type SchemaRegistry struct {
	mu      sync.RWMutex
	schemas map[string]schemaSpec
}

type schemaSpec struct {
	current uint32 // 当前（最新）可生产版本
	min     uint32 // 仍可接受的最旧版本；低于此版本拒绝
	dec     DecoderFunc
}

// DecoderFunc 由使用方提供，负责把某个具体版本的字节迁移/解码为当前结构。
// 返回 *Error（KindVersionMismatch / KindMissingField / KindDecode）表达明确结论；
// 返回其它 error 会被统一映射为 KindDecode，底层异常不透出。
type DecoderFunc func(env *Envelope) (any, error)

// NewRegistry 创建空的 schema 注册表。
func NewRegistry() *SchemaRegistry {
	return &SchemaRegistry{schemas: make(map[string]schemaSpec)}
}

// Register 注册 schema：当前版本、可接受的最旧版本与解码函数。
func (r *SchemaRegistry) Register(schema string, currentVersion, minVersion uint32, dec DecoderFunc) error {
	if schema == "" {
		return wrapError(KindInvalidArgument, "register", "", "empty schema name", nil)
	}
	if dec == nil {
		return wrapError(KindInvalidArgument, "register", "", "nil decoder for "+schema, nil)
	}
	if minVersion > currentVersion {
		return wrapError(KindInvalidArgument, "register", "",
			fmt.Sprintf("minVersion %d > currentVersion %d", minVersion, currentVersion), nil)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.schemas[schema]; exists {
		return wrapError(KindConflict, "register", "", "schema already registered: "+schema, nil)
	}
	r.schemas[schema] = schemaSpec{current: currentVersion, min: minVersion, dec: dec}
	return nil
}

// Encode 将结构体 JSON 编码为指定 schema 当前版本的信封（Seq/ID 由总线补齐）。
func (r *SchemaRegistry) Encode(schema string, v any) (*Envelope, error) {
	r.mu.RLock()
	spec, ok := r.schemas[schema]
	r.mu.RUnlock()
	if !ok {
		return nil, wrapError(KindUnknownSchema, "encode", "", "schema not registered: "+schema, nil)
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, wrapError(KindEncode, "encode", "", "json marshal failed", err)
	}
	return &Envelope{
		Schema:   schema,
		Version:  spec.current,
		Encoding: "json",
		Data:     data,
	}, nil
}

// Decode 按信封上的 schema/版本解码：
//   - schema 未注册      -> KindUnknownSchema
//   - 版本超出兼容窗口   -> KindVersionMismatch
//   - 解码函数返回 *Error -> 原样透传（如 KindMissingField）
//   - 其它底层错误       -> 统一映射为 KindDecode，不直接透出
func (r *SchemaRegistry) Decode(env *Envelope) (any, error) {
	if env == nil {
		return nil, wrapError(KindInvalidArgument, "decode", "", "nil envelope", nil)
	}
	r.mu.RLock()
	spec, ok := r.schemas[env.Schema]
	r.mu.RUnlock()
	if !ok {
		return nil, wrapError(KindUnknownSchema, "decode", env.Topic,
			"schema not registered: "+env.Schema, nil)
	}
	if env.Version < spec.min || env.Version > spec.current {
		return nil, wrapError(KindVersionMismatch, "decode", env.Topic,
			fmt.Sprintf("schema %s version %d outside compatible window [%d,%d]",
				env.Schema, env.Version, spec.min, spec.current), ErrVersionMismatch)
	}
	if env.Encoding != "json" {
		return nil, wrapError(KindDecode, "decode", env.Topic,
			"unsupported encoding: "+env.Encoding, nil)
	}
	v, err := spec.dec(env)
	if err != nil {
		var e *Error
		if errors.As(err, &e) {
			return nil, err
		}
		return nil, wrapError(KindDecode, "decode", env.Topic, "decoder failed", err)
	}
	return v, nil
}

// MigrateJSON 是给 DecoderFunc 使用的辅助函数：先按原始版本解到 raw，
// 再调用 migrate 完成字段迁移/校验。json 语法错误映射为 KindDecode，
// migrate 返回的 *Error 原样保留。
func MigrateJSON(env *Envelope, migrate func(version uint32, raw map[string]json.RawMessage) (any, error)) (any, error) {
	if len(bytes.TrimSpace(env.Data)) == 0 {
		return nil, wrapError(KindDecode, "decode", env.Topic, "empty payload", nil)
	}
	raw := make(map[string]json.RawMessage)
	if err := json.Unmarshal(env.Data, &raw); err != nil {
		return nil, wrapError(KindDecode, "decode", env.Topic, "invalid json", err)
	}
	v, err := migrate(env.Version, raw)
	if err != nil {
		var e *Error
		if errors.As(err, &e) {
			if e.Topic == "" {
				e.Topic = env.Topic
			}
			return nil, e
		}
		return nil, wrapError(KindDecode, "decode", env.Topic, "migration failed", err)
	}
	return v, nil
}

// RequireFields 校验 raw 中必需字段存在且非 null，缺失时返回 KindMissingField。
func RequireFields(env *Envelope, raw map[string]json.RawMessage, fields ...string) error {
	var missing []string
	for _, f := range fields {
		v, ok := raw[f]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			missing = append(missing, f)
		}
	}
	if len(missing) > 0 {
		return wrapError(KindMissingField, "decode", env.Topic,
			fmt.Sprintf("missing required field(s): %v", missing), nil)
	}
	return nil
}
