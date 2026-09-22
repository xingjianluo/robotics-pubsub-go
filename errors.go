package pubsub

import (
	"errors"
	"fmt"
)

// ErrorKind 用于对外区分失败原因，底层异常不透出。
type ErrorKind int

const (
	KindUnknown         ErrorKind = iota
	KindDecode                    // 编码/解码异常
	KindMissingField              // 必填字段缺失
	KindVersionMismatch           // 版本不匹配
	KindOverflow                  // 队列溢出 / 背压丢弃
	KindNoSubscriber              // 主题无订阅者（低延迟等级下）
	KindClosed                    // 总线已关闭
	KindInvalidArgument           // 参数非法
)

func (k ErrorKind) String() string {
	switch k {
	case KindDecode:
		return "decode-error"
	case KindMissingField:
		return "missing-field"
	case KindVersionMismatch:
		return "version-mismatch"
	case KindOverflow:
		return "queue-overflow"
	case KindNoSubscriber:
		return "no-subscriber"
	case KindClosed:
		return "bus-closed"
	case KindInvalidArgument:
		return "invalid-argument"
	default:
		return "unknown"
	}
}

// Error 是对外接口返回的统一错误类型；底层异常被包装在 Err 中，
// 不直接透出到对外接口（调用方应按 Kind 判定）。
type Error struct {
	Kind   ErrorKind
	Topic  string
	Reason string
	Err    error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	s := fmt.Sprintf("pubsub: kind=%s", e.Kind)
	if e.Topic != "" {
		s += " topic=" + e.Topic
	}
	if e.Reason != "" {
		s += ": " + e.Reason
	}
	if e.Err != nil {
		s += " (cause: " + e.Err.Error() + ")"
	}
	return s
}

func (e *Error) Unwrap() error { return e.Err }

func newError(kind ErrorKind, topic, reason string, cause error) *Error {
	return &Error{Kind: kind, Topic: topic, Reason: reason, Err: cause}
}

// IsError 报告 err 是否为指定 Kind 的 *Error。
func IsError(err error, kind ErrorKind) bool {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Kind == kind
	}
	return false
}
