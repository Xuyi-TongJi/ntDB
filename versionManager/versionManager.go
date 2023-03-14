package versionManager

import (
	"myDB/dataManager"
	"myDB/transactions"
	"sync"
)

type VersionManager interface {
	Load(uid int64) Record
	Remove(record Record)
}

type VmImpl struct {
	dm   dataManager.DataManager
	tm   transactions.TransactionManager
	undo Log
}

func (v *VmImpl) Load(uid int64) Record {
	//TODO implement me
	panic("implement me")
}

func (v *VmImpl) Remove(record Record) {
	//TODO implement me
	panic("implement me")
}

func NewVersionManager(path string, memory int64) VersionManager {
	tm := transactions.NewTransactionManagerImpl(path)
	dm := dataManager.OpenDataManager(path, memory, tm)
	undo := OpenUndoLog(path, &sync.Mutex{})
	return &VmImpl{
		dm: dm, tm: tm, undo: undo,
	}
}
