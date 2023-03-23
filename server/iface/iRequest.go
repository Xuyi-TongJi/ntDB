package iface

/*
	IRequest接口
	把客户端请求的连接信息，包装到一个Request中
*/

type IRequest interface {
	// GetConnection 得到当前连接
	GetConnection() IConnection
	// GetDataLen 获取数据的长度
	GetDataLen() uint32
	// GetMsgId 获取数据id
	GetMsgId() uint32
	// GetData 得到请求的消息数据
	GetData() []byte
}
