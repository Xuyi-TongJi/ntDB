package main

import (
	"fmt"
	"myDB/transactions"
	"testing"
)

// Accepted
func TestTransactionManager(t *testing.T) {
	tm := transactions.NewTransactionManagerImpl("test.txt")
	tm.Begin()
	tm.Begin()
	fmt.Println(tm.Status(1))
	fmt.Println(tm.Status(2))
	tm.Abort(1)
	tm.Commit(2)
	fmt.Println(tm.Status(1))
	fmt.Println(tm.Status(2))
	//stat := tm.Debug()
	//fmt.Println(stat.Size())
}
