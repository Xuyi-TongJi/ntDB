package iface

// IServer 服务器接口
type IServer interface {
	Start()
	Stop()
	Serve()
	AddRouter(msgID uint32, router IRouter) // 添加路由
	GetConnectionManager() IConnectionManager
	SetOnConnectionStart(hook func(connection IConnection))
	SetOnConnectionStop(hook func(connection IConnection))
	CallOnConnectionStart(connection IConnection)
	CallOnConnectionStop(connection IConnection)
}
