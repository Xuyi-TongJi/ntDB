package main

import (
	"myDB/server/network"
	"myDB/server/utils"
)

func main() {
	s := network.NewServer("tcp4")
	/* Config Router */
	s.AddRouter(0, &PingRouter{name: "ping router"})
	s.AddRouter(1, &HelloRouter{name: "hello router"})
	/* Config Hook */
	utils.GlobalObj.TcpServer = s
	s.Serve()
}
