package main

import (
	"myDB/dataManager"
	"testing"
)

// ACCEPTED
func TestRedoLog(t *testing.T) {
	_ = dataManager.OpenRedoLog("./test")
}
