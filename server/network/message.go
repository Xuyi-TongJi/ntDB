package network

type Message struct {
	// 消息的ID
	Id uint32

	// 消息长度
	Len uint32

	// 消息的内容
	Data []byte
}

func (m *Message) GetMsgId() uint32 {
	return m.Id
}

func (m *Message) GetLen() uint32 {
	return m.Len
}

func (m *Message) GetData() []byte {
	return m.Data
}

func (m *Message) SetMsgId(id uint32) {
	m.Id = id
}

func (m *Message) SetLen(len uint32) {
	m.Len = len
}

func (m *Message) SetData(data []byte) {
	m.Data = data
}
