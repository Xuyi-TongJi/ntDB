package main

import (
	"log"
	"myDB/bufferPool"
	"sync"
	"testing"
)

// Accepted
func TestRefCountBufferPool1(t *testing.T) {
	pool := bufferPool.NewRefCountBufferPoolImpl(10)
	wg := sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func(key int64) {
			//fmt.Println("key-->", key)
			defer wg.Done()
			_, err := pool.Get(key)
			if err != nil {
				panic(err)
			}
		}(int64(i % 10))
	}
	wg.Wait()
	wg.Add(1000)
	pool.(*bufferPool.RefCountBufferPoolImpl).Debug()
	log.Println()
	for i := 0; i < 1000; i++ {
		go func(key int64) {
			defer wg.Done()
			// 保留key=1
			if key == 1 {
				return
			}
			err := pool.Release(key)
			if err != nil {
				panic(err)
			}
		}(int64(i % 10))
	}
	wg.Wait()
	pool.(*bufferPool.RefCountBufferPoolImpl).Debug()
}

// Accepted
func TestRefCountOOM(t *testing.T) {
	pool := bufferPool.NewRefCountBufferPoolImpl(5)
	wg := sync.WaitGroup{}
	wg.Add(6)
	for i := 0; i < 6; i++ {
		go func(key int64) {
			defer wg.Done()
			_, err := pool.Get(key)
			if err != nil {
				panic(err)
			}
		}(int64(i))
	}
	wg.Wait()
}
