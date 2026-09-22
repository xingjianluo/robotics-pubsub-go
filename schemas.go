package pubsub

import (
	"encoding/json"
)

// PoseSchema 是机器人位姿消息的逻辑 schema 名。
const PoseSchema = "robot.pose"

// Pose 版本兼容窗口：V1 与 V2 均可被消费，新消息以 V2 生产。
const (
	PoseV1 uint32 = 1
	PoseV2 uint32 = 2
)

// RobotPose 是 robot.pose 的当前（V2）领域结构。
// V1: {"x":..,"y":..,"frame":"map"}
// V2: 在 V1 基础上新增必填字段 confidence。
type RobotPose struct {
	X          float64 `json:"x"`
	Y          float64 `json:"y"`
	Frame      string  `json:"frame"`
	Confidence float64 `json:"confidence"`
}

// RegisterRobotSchemas 注册内置示例 schema：演示版本迁移、字段缺失与编码拒绝。
func RegisterRobotSchemas(reg *SchemaRegistry) error {
	return reg.Register(PoseSchema, PoseV2, PoseV1, func(env *Envelope) (any, error) {
		return MigrateJSON(env, func(version uint32, raw map[string]json.RawMessage) (any, error) {
			switch version {
			case PoseV1:
				if err := RequireFields(env, raw, "x", "y"); err != nil {
					return nil, err
				}
				var p RobotPose
				if err := json.Unmarshal(env.Data, &p); err != nil {
					return nil, wrapError(KindDecode, "decode", env.Topic, "v1 unmarshal failed", err)
				}
				if p.Frame == "" {
					p.Frame = "map" // 旧版本缺省值迁移
				}
				p.Confidence = 1.0 // V1 无此字段，迁移为已知默认值
				return p, nil
			case PoseV2:
				if err := RequireFields(env, raw, "x", "y", "frame", "confidence"); err != nil {
					return nil, err
				}
				var p RobotPose
				if err := json.Unmarshal(env.Data, &p); err != nil {
					return nil, wrapError(KindDecode, "decode", env.Topic, "v2 unmarshal failed", err)
				}
				return p, nil
			default:
				return nil, wrapError(KindVersionMismatch, "decode", env.Topic,
					"no migrator for version", ErrVersionMismatch)
			}
		})
	})
}
