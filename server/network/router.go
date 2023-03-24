package network

import "myDB/server/iface"

const DbRouterMsgId = 0x3f

/*
	路由模块，实现路由接口
	不同消息对应不同的处理方式
*/

// BaseRouter 基类 BaseRouter的方法都为空，是因为可以让Router的具体实现更灵活的实现这三个方法
type BaseRouter struct {
	name string
}

func (b *BaseRouter) PreHandle(request iface.IRequest) {}

func (b *BaseRouter) DoHandle(request iface.IRequest) {}

func (b *BaseRouter) PostHandle(request iface.IRequest) {}
