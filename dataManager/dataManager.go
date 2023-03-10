package dataManager

import (
	. "myDB/transactions"
	"sync"
)

const PageNumberDbMeta int64 = 1

type DataManager interface {
	Read(pageId, offset int64) DataItem
	Insert(xid int64, data []byte)
	Close()
}

type DmImpl struct {
	pageCache          PageCache
	pageCtl            PageCtl
	redo               Log
	transactionManager TransactionManager
	metaPage           Page // 数据库元数据页(不会被换出)
}

func (dm *DmImpl) Read(pageID, offset int64) DataItem {
	//TODO implement me
	panic("implement me")
}

func (dm *DmImpl) Insert(xid int64, data []byte) {
	//TODO implement me
	panic("implement me")
}

func (dm *DmImpl) Close() {
	dm.pageCache.Close()
	dm.transactionManager.Close()
	dm.redo.Close()
}

func OpenDataManager(path string, memory int64) DataManager {
	pc := NewPageCacheRefCountFileSystemImpl(uint32(memory/PageSize), path, &sync.Mutex{})
	pageCtl := InitCtl(&sync.Mutex{}, pc)
	redo := OpenRedoLog(path, &sync.Mutex{})
	tx := NewTransactionManagerImpl(path)
	dm := &DmImpl{
		pageCache:          pc,
		pageCtl:            pageCtl,
		redo:               redo,
		transactionManager: tx,
	}
	dm.init()
	return dm
}

func (dm *DmImpl) init() {
	if metaPage, err := dm.pageCache.GetPage(PageNumberDbMeta); err != nil {
		panic(err)
	} else {
		dm.metaPage = metaPage
	}
	// 数据恢复
	if !dm.metaPage.CheckInitVersion() {
		dm.redo.CrashRecover(dm.pageCache, dm.transactionManager)
	}
	dm.pageCtl.Init(dm.pageCache)
	dm.metaPage.CheckInitVersion()
	dm.pageCache.DoFlush(dm.metaPage)
}
