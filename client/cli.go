package main

import (
	"fmt"
	"myDB/client/cli_core"
	"net"
	"os"
)

func main() {
	fmt.Println("Client start..")
	if len(os.Args) < 2 {
		fmt.Println("Invalid tcp server address")
		return
	}
	serverAddr := os.Args[1]
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Printf("[ERROR] Connection errer:%s\n", err)
		return
	}
	fmt.Printf("Connection to server %s success!\n", serverAddr)
	// 阻塞通道
	stop := make(chan error)
	go cli_core.StartReader(conn, stop)
	go cli_core.StartWriter(conn, stop)
	select {
	case err = <-stop:
		if err.Error() == "EOF" {
			fmt.Printf("See you next time\n")
		} else {
			fmt.Printf("[ERROR] Fatel error:%s\n", err)
		}
	}
}
