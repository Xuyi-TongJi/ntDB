package iface

// 消息管理模块抽象层

type IConnectionManager interface {
	Add(c IConnection)
	Remove(c IConnection)
	Get(c uint32) (IConnection, error)
	Total() int
	ClearAll()
}
