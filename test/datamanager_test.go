package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// // Create a dataManager
// // ACCEPTED
//
//	func TestDm(t *testing.T) {
//		_ = dataManager.OpenDataManager("test", 1<<16)
//	}
//
// // Close a dataManager and reopen a dataManager
// // ACCEPTED
//
//	func TestDm2(t *testing.T) {
//		dm := dataManager.OpenDataManager("test", 1<<16)
//		dm.Close()
//		//dm = dataManager.OpenDataManager("test", 1<<16)
//		//dm.Close()
//	}
func TestByte(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int64(100000))
	buffer := buf.Bytes()
	fmt.Println(int64(binary.BigEndian.Uint64(buffer[0:8])))
}
