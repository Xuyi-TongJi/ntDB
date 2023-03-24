package network

type Message struct {
	// 消息的ID
	Id   uint32
	args []string // args
}

func (m *Message) GetMsgId() uint32 {
	return m.Id
}

func (m *Message) GetArgs() []string {
	return m.args
}
