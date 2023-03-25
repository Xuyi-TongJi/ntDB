package network

import "myDB/server/iface"

type Request struct {
	// 已经和客户端建立好的连接
	conn    iface.IConnection
	message iface.IMessage
}

func (r *Request) GetConnection() iface.IConnection {
	return r.conn
}

func (r *Request) GetArgs() []string {
	return r.message.GetArgs()
}

func (r *Request) GetMsgId() uint32 {
	return r.message.GetMsgId()
}
