package main

import (
	"fmt"
	"myDB/dataManager"
	"os"
	"sync"
	"testing"
)

func TestLock(t *testing.T) {
	var lock sync.Mutex
	var x = 0
	wg := sync.WaitGroup{}
	todo := func(l *sync.Mutex) {
		l.Lock()
		defer func() {
			l.Unlock()
			wg.Done()
		}()
		x++
	}
	wg.Add(10000)
	for i := 0; i < 10000; i++ {
		go todo(&lock)
	}
	wg.Wait()
	fmt.Println(x)
}

// ACCEPTED
func TestPageCache(t *testing.T) {
	pc := dataManager.NewPageCacheRefCountFileSystemImpl(10, "test.txt")
	if err := pc.TruncateDataSource(1); err != nil {
		panic(err)
	}
	fmt.Println(pc.GetPageNumbers())
	buf := []byte("hello world")
	pc.NewPage(buf)
	fmt.Println(pc.GetPageNumbers())
	page, _ := pc.GetPage(2)
	fmt.Println(string(page.GetData()[:11]))
}

func TestFile(t *testing.T) {
	file, _ := os.OpenFile("test.txt", os.O_RDWR, 0666)
	file.Write([]byte("hello world"))
	//file.Truncate(5)
	buf := make([]byte, 10)
	bb := make([]byte, 5)
	n, _ := file.Read(buf)
	fmt.Println(n)
	n, _ = file.Read(bb)
	fmt.Println(n)
	fmt.Println(string(buf))
	fmt.Println(string(bb))
}
