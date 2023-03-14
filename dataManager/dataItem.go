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
	GetPage() Page
	GetUid() int64

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
	page   Page // BufferPool中的页, 当前raw位于哪个页
	uid    int64
	dm     DataManager
	raw    []byte
	oldRaw []byte
	lock   *sync.RWMutex
}

const (
	SzDIValid    int64 = 1
	SzDIDataSize int64 = 8
)

func NewDataItem(raw []byte, oldRaw []byte, lock *sync.RWMutex, dm DataManager,
	page Page, uid int64) DataItem {
	return &DataItemImpl{
		page:   page,
		uid:    uid,
		dm:     dm,
		raw:    raw,
		oldRaw: oldRaw,
		lock:   lock,
	}
}

func (di *DataItemImpl) GetData() []byte {
	return di.raw[SzDIValid+SzDIDataSize:]
}

func (di *DataItemImpl) GetRaw() []byte {
	return di.raw
}

func (di *DataItemImpl) GetOldRaw() []byte {
	return di.oldRaw
}

func (di *DataItemImpl) GetUid() int64 {
	return di.uid
}

func (di *DataItemImpl) IsValid() bool {
	return di.raw[0] == 1
}

func (di *DataItemImpl) SetInvalid() {
	copy(di.raw[:SzDIValid], []byte{byte(1)})
}

func (di *DataItemImpl) GetPage() Page {
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

// WrapDataItemRaw
// RAW: [valid]1[dataSize]8[data]
func wrapDataItemRaw(raw []byte) []byte {
	size := int64(len(raw))
	valid := int8(1)
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.BigEndian, valid)
	_ = binary.Write(buffer, binary.BigEndian, size)
	_ = binary.Write(buffer, binary.BigEndian, raw)
	return buffer.Bytes()
}
