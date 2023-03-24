package iface

/*
	IRequest接口
	把客户端请求的连接信息，包装到一个Request中
*/

type IRequest interface {
	GetConnection() IConnection
	GetArgs() []string
}
