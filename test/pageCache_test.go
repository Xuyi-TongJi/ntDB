package main

import (
	"fmt"
	"myDB/dataManager"
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

func TestPageCache(t *testing.T) {
	_ = dataManager.NewPageCacheRefCountFileSystemImpl(10, "test.txt")
	//pc.GetPage(1)
}
