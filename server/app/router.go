package main

import (
	"myDB/executor"
	"myDB/server/iface"
	"myDB/server/network"
)

// DbRouter 自定义Router
type DbRouter struct {
	network.BaseRouter
	name string
	db   executor.Executor
	/*
	   transActions cmap
	   autoCommitted cmap
	*/
}

func (dbRouter *DbRouter) DoHandle(request iface.IRequest) {

}

func NewDbRouter(path string, memory int64) iface.IRouter {
	return nil
}
