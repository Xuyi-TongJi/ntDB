package iface

import "net"

// IConnection 连接模块的抽象层
type IConnection interface {
	// Start 启动连接，让当前连接准备开始工作
	Start()
	// Stop 停止连接，结束当前连接的工作
	Stop()
	// GetTcpConnection 获取当前连接所绑定的socket connection（套接字）
	GetTcpConnection() *net.TCPConn
	// GetConnId 获取当前连接模块的连接ID
	GetConnId() uint32
	// GetClientTcpStatus 获取客户端（对端）的TCP状态
	GetClientTcpStatus() net.Addr
	// SendMessage 发送数据，将数据封包为Message并发送给远程的客户端
	SendMessage(data []byte)
	// SetConnectionProperty 设置连接属性
	SetConnectionProperty(key string, value interface{})
	// GetConnectionProperty 获得连接属性
	GetConnectionProperty(key string) interface{}
	// RemoveConnectionProperty 移除连接属性
	RemoveConnectionProperty(key string)
	// HasClosed 连接是否关闭
	HasClosed() bool
	GetMsgHandler() IMessageHandler
}

// HandleFunc 定义一个处理连接业务的方法
type HandleFunc func(IConnection, []byte, int) error
