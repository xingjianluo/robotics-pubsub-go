package pubsub

// ErrorKind 是对外错误类别，底层异常一律映射到这些类别，不直接透出。
type ErrorKind string

const (
	KindEncode          ErrorKind = "encode_error"
	KindDecode          ErrorKind = "decode_error"
	KindMissingField    ErrorKind = "missing_field"
	KindVersionMismatch ErrorKind = "version_mismatch"
	KindUnknownSchema   ErrorKind = "unknown_schema"
	KindBackpressure    ErrorKind = "backpressure"
	KindClosed          ErrorKind = "closed"
	KindConflict        ErrorKind = "conflict"
	KindInvalidArgument ErrorKind = "invalid_argument"
)

// Error 是总线对外暴露的唯一错误形态。底层编解码/容器异常都包装为该类型，
// 调用方通过 Kind 即可区分原因，无需依赖底层实现细节。
type Error struct {
	Kind   ErrorKind
	Op     string // 触发错误的操作，如 "publish"、"decode"
	Topic  string // 相关主题（可能为空）
	Detail string // 人类可读的补充说明
	Err    error  // 被包装的底层错误（仅用于 Unwrap/日志，不直接透出）
}

func (e *Error) Error() string {
	s := string(e.Kind)
	if e.Op != "" {
		s += " [" + e.Op + "]"
	}
	if e.Topic != "" {
		s += " topic=" + e.Topic
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	if e.Err != nil {
		s += " (cause: " + e.Err.Error() + ")"
	}
	return s
}

func (e *Error) Unwrap() error { return e.Err }

func wrapError(kind ErrorKind, op, topic, detail string, cause error) *Error {
	return &Error{Kind: kind, Op: op, Topic: topic, Detail: detail, Err: cause}
}

// 哨兵错误，便于 errors.Is 判定。
var (
	ErrQueueFull       = &Error{Kind: KindBackpressure, Detail: "subscriber queue full"}
	ErrBusClosed       = &Error{Kind: KindClosed, Detail: "bus closed"}
	ErrSubNotFound     = &Error{Kind: KindInvalidArgument, Detail: "subscription not found"}
	ErrAlreadyExists   = &Error{Kind: KindConflict, Detail: "subscription already exists"}
	ErrVersionMismatch = &Error{Kind: KindVersionMismatch}
)
