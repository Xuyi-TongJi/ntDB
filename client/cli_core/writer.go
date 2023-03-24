package cli_core

import (
	"bufio"
	"net"
	"os"
)

// test for bulk request
// const s string = "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"

func StartWriter(conn net.Conn, stop chan error) {
	reader := bufio.NewReader(os.Stdin)
	for {
		bytes, _, err := reader.ReadLine()
		if err != nil {
			stop <- err
			break
		}
		// INLINE
		// BULK Request is also supported
		length := len(bytes)
		bytes = append(bytes, '\r')
		bytes = append(bytes, '\n')
		index := 0
		for index < length {
			n, err := conn.Write(bytes[index:])
			if err != nil {
				stop <- err
				break
			}
			index += n
		}
		// log.Printf("[CLIENT WRITER] Send message %s success\n", s)
	}
}
