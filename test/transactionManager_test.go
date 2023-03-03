package main

import (
	"myDB/transactions"
	"testing"
)

func TestTransactionManager(t *testing.T) {
	_ = transactions.NewTransactionManagerImpl("test.txt")
}
