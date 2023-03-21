package indexManager

import (
	"myDB/versionManager"
)

type IndexManager interface {
	LoadIndex(indexUid int64) Index
	CreateIndex(xid int64, compare func(any, any) int) int64
}

type IMImpl struct {
	vm versionManager.VersionManager
}

func (im *IMImpl) LoadIndex(indexUid int64) Index {
	//TODO implement me
	panic("implement me")
}

func (im *IMImpl) CreateIndex(xid int64, compare func(any, any) int) int64 {
	//TODO implement me
	panic("implement me")
}

func NewIndexManager(vm versionManager.VersionManager) IndexManager {
	return &IMImpl{
		vm: vm,
	}
}
