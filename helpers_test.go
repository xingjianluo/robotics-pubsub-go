package pubsub

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// captureLogger 同时输出到测试日志并保留一份内存副本，便于失败复现时打印判定依据。
// 只在 busClosed 未置位时调用 t.Log：测试一律先 bus.Close()（同步等待各会话终止），
// Close 内会先置位该标志，因此 Close 返回后不再有任何 t.Log，杜绝与 testing.T
// 收尾之间的数据竞争。
type captureLogger struct {
	t         *testing.T
	busClosed *atomic.Bool
	mu        sync.Mutex
	buf       []string
}

func newCaptureLogger(t *testing.T) *captureLogger {
	return &captureLogger{t: t, busClosed: &atomic.Bool{}}
}

func (c *captureLogger) Printf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	full := time.Now().Format("15:04:05.000000") + " " + line
	c.mu.Lock()
	c.buf = append(c.buf, full)
	c.mu.Unlock()
	if !c.busClosed.Load() {
		c.t.Log(line) // 单测日志中打印关键会话与投递判定依据
	}
}

func (c *captureLogger) Lines() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.buf))
	copy(out, c.buf)
	return out
}

func (c *captureLogger) DumpOnFailure(t *testing.T) {
	t.Helper()
	if t.Failed() {
		t.Logf("---- captured bus log (%d lines) ----", len(c.buf))
		for _, l := range c.Lines() {
			t.Log(l)
		}
	}
}

// mustWithin 在 d 时间内等待 cond 为真，超时则失败。
func mustWithin(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timeout waiting: %s", msg)
	}
}

func newTestBus(t *testing.T, opts ...Option) *Bus {
	t.Helper()
	reg := NewRegistry()
	if err := RegisterRobotSchemas(reg); err != nil {
		t.Fatal(err)
	}
	cl := newCaptureLogger(t)
	all := append([]Option{WithLogger(cl), withClosingFlag(cl.busClosed)}, opts...)
	return New(reg, all...)
}
