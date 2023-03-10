package main

import (
	"myDB/dataManager"
	"testing"
)

func TestDm(t *testing.T) {
	_ = dataManager.OpenDataManager("test", 1<<16)
}
