package main

import (
	"myDB/server/iface"
	"myDB/server/network"
)

// PingRouter 自定义Router
// Router 就是服务器所能提供的服务
type PingRouter struct {
	network.BaseRouter
	name string
}

// PreHandle Override
/*func (r *PingRouter) PreHandle(req iface.IRequest) {
	fmt.Println("[Router PreHandle] Call router pre handle")
	// socket
	_, err := req.GetConnection().GetTcpConnection().Write([]byte("before handle\n"))
	if err != nil {
		fmt.Printf("[Router PreHandle] Call back before handle, error:%s\n", err)
	}
}
*/

// DoHandle Override
func (r *PingRouter) DoHandle(req iface.IRequest) {

}

// PostHandle Override
/*func (r *PingRouter) PostHandle(req iface.IRequest) {
	fmt.Println("[Router PostHandle] Call router post handle")
	_, err := req.GetConnection().GetTcpConnection().Write([]byte("after handle\n"))
	if err != nil {
		fmt.Printf("[Router PostHandle] Call back after handle, error:%s\n", err)
	}
	time.Sleep(5 * time.Second)
}*/

type HelloRouter struct {
	network.BaseRouter
	name string
}

func (r *HelloRouter) DoHandle(req iface.IRequest) {

}
