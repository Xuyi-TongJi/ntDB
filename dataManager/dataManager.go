package dataManager

import (
	"encoding/binary"
	"fmt"
	"log"
	. "myDB/transactions"
	"sync"
)

// DataManager 管理PageCache(BufferPool+Data Source), Page Control, RedoLog
// 上层请求必须保证请求的长度八字节对齐

const PageNumberDbMeta int64 = 1

type DataManager interface {
	Read(uid int64) DataItem
	ReadSnapShot(uid int64) DataItem
	Update(xid, uid int64, data []byte) int64
	Insert(xid int64, data []byte) int64
	Delete(xid, uid int64)
	Recover(xid, uid int64) // 回复删除(set valid)
	Release(id DataItem)
	Close()
}

type DmImpl struct {
	pageCache          PageCache
	pageCtl            PageCtl
	redo               Log
	transactionManager TransactionManager
	metaPage           Page // 数据库元数据页(直到dataManager关闭不会被换出)
}

// ReadSnapShot
// 根据uid读取DataItem, 不检验有效位, 直接返回
// 一定不会返回nil
// 应用场景：快照读
func (dm *DmImpl) ReadSnapShot(uid int64) DataItem {
	return dm.doRead(uid)
}

// Read
// 根据uid从PC中读取DataItem并校验有效位
// 当DataItem失效时，返回nil
// 应用场景：当前读
func (dm *DmImpl) Read(uid int64) DataItem {
	di := dm.doRead(uid)
	if !di.IsValid() {
		return nil
	}
	return di
}

func (dm *DmImpl) doRead(uid int64) DataItem {
	pageId, offset := uidTrans(uid)
	if page, err := dm.pageCache.GetPage(pageId); err != nil {
		panic(fmt.Sprintf("Error occurs when getting pages, err = %s", err))
	} else {
		item := dm.getDataItem(page, offset)
		dm.Release(item)
		return item
	}
}

// Update
// 更新数据
// 尝试更新失效的或者不存在的数据时，panic
// 更新的数据长度小于，原地更新，否则将当前DataItem设置为无效，并且新插入一个DataItem
// 返回新数据的地址
// 上层模块保证其操作的安全性（VersionManager）
func (dm *DmImpl) Update(xid, uid int64, data []byte) int64 {
	di := dm.Read(uid)
	if di == nil {
		panic("Error occurs when updating data item, this data item is invalid")
	}
	oldRaw := di.GetRaw()
	newRaw := WrapDataItemRaw(data)
	var ret int64
	if len(oldRaw) >= len(newRaw) {
		// 原地更新
		// LOG FIRST
		dm.redo.UpdateLog(uid, xid, oldRaw, newRaw)
		di.Update(newRaw)
		ret = uid
	} else {
		// DELETE
		dm.Delete(xid, uid)
		// INSERT
		ret = dm.Insert(xid, data)
	}
	di.Release()
	return ret
}

// Insert
// 申请向Page Cache插入一段数据
// log first and insert next
// return uid(pageId, offset)
func (dm *DmImpl) Insert(xid int64, data []byte) int64 {
	// wrap
	raw := WrapDataItemRaw(data)
	length := int64(len(raw))
	if length > MaxFreeSize {
		// 暂不支持跨页存储
		panic("Error occurs when inserting data, err = data length overflow\n")
	}
	// find a free page by page Ctl
	pi := dm.pageCtl.Select(length)
	var pageId int64
	// if necessarily, create a new page
	if pi == nil {
		pageId = dm.pageCache.NewPage(DataPage)
	} else {
		pageId = pi.PageId
	}
	pg, err := dm.pageCache.GetPage(pageId)
	if err != nil {
		panic(fmt.Sprintf("Error occurs when getting page, err = %s", err))
	}
	offset := pg.GetUsed()
	// LOG FIRST
	log.Printf("[Data Manager LINE 123] locate at %d %d\n", pg.GetId(), offset)
	dm.redo.InsertLog(getUid(pg.GetId(), offset), xid, raw)
	log.Printf("[Data Manager LINE 125] finish log %d %d\n", pg.GetId(), offset)
	// update page data
	// TODO
	if err := pg.Append(raw); err != nil {
		panic(fmt.Sprintf("Error occurs when updating page, err = %s\n", err))
	}
	log.Printf("[Data Manager LINE 131] finish append %d %d\n", pg.GetId(), offset)
	// update pageCtl
	dm.pageCtl.AddPageInfo(pg.GetId(), pg.GetFree())
	// release
	if err := dm.pageCache.ReleasePage(pg); err != nil {
		panic(fmt.Sprintf("Error occurs when releasing page, err = %s\n", err))
	}
	return getUid(pg.GetId(), offset)
}

func (dm *DmImpl) Release(di DataItem) {
	if err := dm.pageCache.ReleasePage(di.GetPage()); err != nil {
		panic(err)
	}
}

// Delete
// 删除一个DataItem(set invalid)
// 对于已经删除的DI，不进行任何操作
func (dm *DmImpl) Delete(xid, uid int64) {
	di := dm.Read(uid)
	if di != nil {
		// LOG FIRST
		oldRaw := di.GetRaw()
		newRaw := make([]byte, len(oldRaw))
		copy(newRaw, oldRaw)
		SetRawInvalid(newRaw)
		dm.redo.UpdateLog(uid, xid, oldRaw, newRaw)
		di.SetInvalid()
	}
	di.Release()
}

// Recover
// 恢复已经删除的DataItem (set valid)
// 对于已经valid的DI，不进行任何操作
func (dm *DmImpl) Recover(xid, uid int64) {
	pageId, offset := uidTrans(uid)
	if page, err := dm.pageCache.GetPage(pageId); err != nil {
		panic(fmt.Sprintf("Error occurs when getting pages, err = %s", err))
	} else {
		di := dm.getDataItem(page, offset)
		if !di.IsValid() {
			// LOG FIRST
			oldRaw := di.GetRaw()
			newRaw := make([]byte, len(oldRaw))
			copy(newRaw, oldRaw)
			SetRawValid(newRaw)
			dm.redo.UpdateLog(uid, xid, oldRaw, newRaw)
			di.SetValid()
		}
		di.Release()
	}
}

func (dm *DmImpl) Close() {
	dm.transactionManager.Close()
	dm.redo.Close()
	dm.metaPage.UpdateVersion()
	if err := dm.pageCache.ReleasePage(dm.metaPage); err != nil {
		panic(fmt.Sprintf("Error occurs when releasing db meta page, err = %s", err))
	}
	dm.pageCache.Close()
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
	// 重置日志文件
	dm.redo.ResetLog()
	// 初始化版本号
	dm.metaPage.InitVersion()
	dm.pageCache.DoFlush(dm.metaPage)
	log.Printf("[Data Manager] Initialze page cache\n")
	dm.pageCtl.Init(dm.pageCache)
}

// getDataItem
// get DataItem from the dataManger by the page
func (dm *DmImpl) getDataItem(page Page, offset int64) DataItem {
	// start from the offset of data
	data := page.GetData()
	// RAW [valid]1[size]8[data]
	dataSize := int64(binary.BigEndian.Uint64(data[offset+SzDIValid : offset+SzDIValid+SzDIDataSize]))
	raw := data[offset : offset+SzDIValid+SzDIDataSize+dataSize]
	uid := getUid(page.GetId(), offset)
	// raw直接引用给DataItem
	return NewDataItem(raw, dm, page, uid)
}

// uid 高32位为pageId, 低32位为offset
func uidTrans(uid int64) (pageId, offset int64) {
	offset = uid & ((1 << 32) - 1)
	uid >>= 32
	pageId = uid & ((1 << 32) - 1)
	log.Printf("[Data Manager] UID TRANS LOCATE AT %d %d\n", pageId, offset)
	return
}

func getUid(pageId, offset int64) int64 {
	return (pageId << 32) | offset
}

func OpenDataManager(path string, memory int64, tm TransactionManager) DataManager {
	pc := NewPageCacheRefCountFileSystemImpl(uint32(memory/PageSize), path, &sync.Mutex{})
	pageCtl := NewPageCtl(&sync.Mutex{}, pc)
	redo := OpenRedoLog(path, &sync.Mutex{})
	dm := &DmImpl{
		pageCache:          pc,
		pageCtl:            pageCtl,
		redo:               redo,
		transactionManager: tm,
	}
	dm.init()
	log.Printf("[Data Manager] Initialize data manager\n")
	return dm
}
