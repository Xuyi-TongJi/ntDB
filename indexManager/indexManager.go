package indexManager

import "myDB/dataManager"

type IndexManager interface {
	LoadIndex(indexUid int64) Index
	CreateIndex(compare func(any, any) int) int64
}

type IMImpl struct {
	dm dataManager.DataManager
}
