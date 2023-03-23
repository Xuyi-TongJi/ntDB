package iface

/*
	IMessage接口封装请求消息
*/

type IMessage interface {
	GetMsgId() uint32

	GetLen() uint32

	GetData() []byte

	SetMsgId(id uint32)

	SetLen(len uint32)

	SetData(data []byte)
}
