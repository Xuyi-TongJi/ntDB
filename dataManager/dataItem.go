package dataManager

import (
	"bytes"
	"encoding/binary"
	"sync"
)

// DataItem
// DataManager 向上层提供的抽象, 上层模块调用
// 上层模块通过地址信息向DataManager请求得到DataItem
// DataItem和Page类似于Golang MMU中的mspan和page的关系
type DataItem interface {
	GetData() []byte
	GetRaw() []byte
	GetOldRaw() []byte
	IsValid() bool
	SetInvalid()
	Release()
	Page() Page
	GetPageId() int64
	GetOffset() int64

	// lock

	Lock()
	UnLock()
	RLock()
	RUnLock()

	// update

	BeforeUpdate(xid int64)
	UndoUpdate(xid int64)
	AfterUpdate(xid int64)
}

// DataItemImpl
// RAW: [valid]1[dataSize]8[data]
type DataItemImpl struct {
	page   Page // BufferPool中的页
	pageId int64
	offset int64
	dm     DataManager
	raw    []byte
	oldRaw []byte
	lock   *sync.RWMutex
	redo   Log
}

const (
	SzValid    int = 1
	SzDataSize int = 8
)

func NewDataItem(data []byte, lock *sync.RWMutex, redo Log, dm DataManager,
	page Page, pageId int64, offset int64) DataItem {
	buffer := bytes.NewBuffer(make([]byte, 0))
	_ = binary.Write(buffer, binary.BigEndian, byte(1))
	_ = binary.Write(buffer, binary.BigEndian, int64(len(data)))
	_ = binary.Write(buffer, binary.BigEndian, data)
	return &DataItemImpl{
		page:   page,
		pageId: pageId,
		offset: offset,
		dm:     dm,
		raw:    buffer.Bytes(),
		oldRaw: nil,
		lock:   lock,
		redo:   redo,
	}
}

func (di *DataItemImpl) GetData() []byte {
	return di.raw[SzValid+SzDataSize:]
}

func (di *DataItemImpl) GetRaw() []byte {
	return di.raw
}

func (di *DataItemImpl) GetOldRaw() []byte {
	return di.oldRaw
}

func (di *DataItemImpl) GetPageId() int64 {
	return di.pageId
}

func (di *DataItemImpl) GetOffset() int64 {
	return di.offset
}

func (di *DataItemImpl) IsValid() bool {
	return di.raw[0] == 1
}

func (di *DataItemImpl) SetInvalid() {
	copy(di.raw[:SzValid], []byte{byte(1)})
}

func (di *DataItemImpl) Release() {
	// TODO
}

func (di *DataItemImpl) Page() Page {
	return di.page
}

func (di *DataItemImpl) Lock() {
	di.lock.Lock()
}

func (di *DataItemImpl) UnLock() {
	di.lock.Unlock()
}

func (di *DataItemImpl) RLock() {
	di.lock.RLock()
}

func (di *DataItemImpl) RUnLock() {
	di.lock.RUnlock()
}

func (di *DataItemImpl) BeforeUpdate(xid int64) {
	// TODO
}

func (di *DataItemImpl) UndoUpdate(xid int64) {
	//TODO implement me
	panic("implement me")
}

func (di *DataItemImpl) AfterUpdate(xid int64) {
	//TODO implement me
	panic("implement me")
}

func (di *DataItemImpl) ParseDataItem(pageId, offset int64) DataItem {
	// TODO
	return nil
}
