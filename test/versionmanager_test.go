package main

import (
	"myDB/versionManager"
	"sync"
	"testing"
)

func TestVm(t *testing.T) {
	_ = versionManager.NewVersionManager("test", 1<<16, &sync.RWMutex{}, 1)
}
