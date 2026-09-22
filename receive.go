package pubsub

import "context"

// Receive 从订阅读取下一条消息并按主题注册的 schema 解码负载。
//
// 返回的 value 为 Decode 后的结果（JSONCodec 下为业务 JSON 原始字节）。
// 解码失败时：
//   - KindVersionMismatch：消息版本不在主题可接受版本集合；
//   - KindMissingField：必填字段缺失；
//   - KindDecode：负载非法、帧/JSON 异常。
//
// 底层异常被包装在 *Error.Err 中，不直接透出。
// 总线/订阅关闭且通道耗尽时返回 (nil, KindClosed)。
func (s *Subscription) Receive(ctx context.Context) (*Message, any, error) {
	if s.st == nil {
		return nil, nil, newError(KindInvalidArgument, s.topic, "nil subscription", nil)
	}
	ch := s.Messages()
	var d *Delivery
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case dd, ok := <-ch:
		if !ok || dd == nil {
			return nil, nil, newError(KindClosed, s.topic, "subscription channel closed", nil)
		}
		d = dd
	}
	// 标记该消息已离开通道（未 Ack 仍保留在 pending，可重发/续传），
	// 并通知投递协程通道空出一个槽位。
	if s.st != nil {
		s.st.mu.Lock()
		if q, ok := s.st.pending[d.Msg.Seq]; ok {
			q.inCh = false
		}
		s.st.signalLocked() // 可能触发该未 Ack 消息的重发判定
		s.st.mu.Unlock()
	}
	value, err := s.bus.decodeDelivery(d)
	if err != nil {
		return d.Msg, nil, err
	}
	return d.Msg, value, nil
}

func (b *Bus) decodeDelivery(d *Delivery) (any, error) {
	msg := d.Msg
	b.mu.RLock()
	schema := b.schemas[msg.Topic]
	b.mu.RUnlock()
	if schema == nil {
		// 未注册 schema：原样返回负载字节。
		return msg.Payload, nil
	}
	// 版本可接受性校验（decode 内亦校验，此处保证错误 Kind 明确）。
	if jc, ok := b.codec.(*JSONCodec); ok {
		if set := jc.AcceptVersions[msg.Topic]; len(set) > 0 && !set[msg.Version] {
			return nil, newError(KindVersionMismatch, msg.Topic,
				"delivered message version not accepted", nil)
		}
	}
	value, err := b.codec.Decode(context.Background(), schema, msg.Payload)
	if err != nil {
		return nil, classifyEncodeErr(msg.Topic, err)
	}
	return value, nil
}
