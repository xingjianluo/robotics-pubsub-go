package pubsub

import "time"

// nackBackoff 是高可靠消息被 Nack 后重投前的短暂退避，避免异常处理器忙等。
const nackBackoff = 5 * time.Millisecond
