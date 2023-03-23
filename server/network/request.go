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

func (r *Request) GetData() []byte {
	return r.message.GetData()
}

func (r *Request) GetDataLen() uint32 {
	return r.message.GetLen()
}

func (r *Request) GetMsgId() uint32 {
	return r.message.GetMsgId()
}
