package iface

/*
	IMessage接口封装请求消息
*/

type IMessage interface {
	GetMsgId() uint32

	GetArgs() []string
}
