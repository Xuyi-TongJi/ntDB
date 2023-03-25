package main

import (
	"errors"
	"io"
	"log"
	"myDB/client/cli_core"
	"net"
	"os"
)

func main() {
	log.Println("Client start..")
	if len(os.Args) < 2 {
		log.Println("Invalid tcp server address")
		return
	}
	serverAddr := os.Args[1]
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Printf("[ERROR] Connection errer:%s\n", err)
		return
	}
	log.Printf("Connection to server %s success!\n", serverAddr)
	// 阻塞通道
	stop := make(chan error)
	go cli_core.StartReader(conn, stop)
	go cli_core.StartWriter(conn, stop)
	select {
	case err = <-stop:
		if errors.Is(err, io.EOF) {
			log.Printf("See you next time\n")
		} else {
			log.Printf("[ERROR] Fatel error:%s\n", err)
		}
	}
}
