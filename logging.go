package pubsub

import (
	"log"
	"os"
)

// Logger 是总线使用的最小日志接口。默认输出带前缀的关键会话与投递判定，
// 测试可替换为捕获型实现以复现失败原因。
type Logger interface {
	Printf(format string, args ...any)
}

type stdLogger struct{ l *log.Logger }

func (s *stdLogger) Printf(format string, args ...any) {
	s.l.Printf(format, args...)
}

// discardLogger 不输出任何日志。
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// defaultLogger 输出到 stderr，带 "pubsub: " 前缀。
func defaultLogger() Logger {
	return &stdLogger{l: log.New(os.Stderr, "pubsub: ", log.LstdFlags|log.Lmicroseconds)}
}
