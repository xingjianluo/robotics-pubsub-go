package pubsub

// Version 消息 schema 版本。
type Version uint16

// Message 是总线上传输的消息单元（解码后的对外形态）。
type Message struct {
	ID      string
	Topic   string
	Version Version
	// Payload 为业务负载的原始字节，具体解码由 Codec 负责。
	Payload []byte
	// Seq 是该消息在主题内的单调递增序号，由总线在发布时分配。
	Seq uint64
}

// Envelope 是编码后的线上形态（仅内部使用）。
type Envelope struct {
	ID      string
	Topic   string
	Version Version
	Seq     uint64
	Body    []byte
}

// Delivery 投递给订阅者的记录。
type Delivery struct {
	Msg     *Message
	Attempt int
}
