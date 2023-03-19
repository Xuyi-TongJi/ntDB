package main

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func Test1(t *testing.T) {
	arr1 := [10]int{}
	arr2 := arr1[5:]
	arr2[1] = 10
	fmt.Println(arr1)
	fmt.Println(arr2)
	// Conclusion 即使数组是值类型，切片操作依然共享同一片内存
}

func Test2(t *testing.T) {
	f, _ := os.Create("t.txt")
	buf := []byte("hello")
	//if _, err := f.ReadAt(buf, 1); err != nil {
	//	panic(err)
	//}
	_, _ = f.Write(buf)
	_ = f.Truncate(3)
}

func Test3(t *testing.T) {
	var lock sync.Mutex
	lock.Lock()
	defer lock.Unlock()
	lock.Lock()
}
