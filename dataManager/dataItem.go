package dataManager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

// DataItem
// DataManager 向上层提供的抽象, 上层模块调用
// 上层模块通过地址信息向DataManager请求得到DataItem
// DataItem和Page类似于Golang MMU中的mspan和page的关系
type DataItem interface {
	GetData() []byte
	GetDataLength() int64
	GetRaw() []byte

	IsValid() bool
	SetInvalid()
	SetValid()
	GetPage() Page
	GetUid() int64
	Release()
	Update(newRaw []byte)

	// Deprecated
	// lock

	//Lock()
	//UnLock()
	//RLock()
	//RUnLock()
	//GetOldRaw() []byte

	// update
	//BeforeUpdate(xid int64)
	//UndoUpdate(xid int64)
	//AfterUpdate(xid int64)
}

// DataItemImpl
// RAW: [valid]1[dataSize]8[data]
type DataItemImpl struct {
	page Page // BufferPool中的页, 当前raw位于哪个页
	uid  int64
	dm   DataManager
	raw  []byte // raw是Page[offset:offset+raw_length]的一段切片，修改raw将直接修改page上的数据
	//oldRaw []byte
	lock *sync.RWMutex // 保留字段，可以移除
}

const (
	DIInvalid    byte  = 0
	DIValid      byte  = 1
	SzDIValid    int64 = 1
	SzDIDataSize int64 = 8
)

func NewDataItem(raw []byte, oldRaw []byte, lock *sync.RWMutex, dm DataManager,
	page Page, uid int64) DataItem {
	return &DataItemImpl{
		page: page,
		uid:  uid,
		dm:   dm,
		raw:  raw,
		//oldRaw: oldRaw, // 暂时移除
		lock: lock, // 可以移除
	}
}

// GetData
// 获得DataItem中的载荷DATA
// 深拷贝
// [valid]1[Length]8[DATA]... -> [DATA]
func (di *DataItemImpl) GetData() []byte {
	//di.lock.RLock()
	//defer di.lock.RUnlock()
	data := di.raw[SzDIValid+SzDIDataSize:]
	copyData := make([]byte, len(data))
	copy(copyData, data)
	return copyData
}

func (di *DataItemImpl) GetDataLength() int64 {
	//di.lock.RLock()
	//defer di.lock.RUnlock()
	length := di.raw[SzDIValid : SzDIValid+SzDIDataSize]
	return int64(binary.BigEndian.Uint64(length))
}

// GetRaw
// 获得DataItem Raw [Valid]1[Length]8[Data]
// 深拷贝
func (di *DataItemImpl) GetRaw() []byte {
	//di.lock.RLock()
	//defer di.lock.RUnlock()
	copyData := make([]byte, len(di.raw))
	copy(copyData, di.raw)
	return copyData
}

//func (di *DataItemImpl) GetOldRaw() []byte {
//	//return di.oldRaw
//}

func (di *DataItemImpl) GetUid() int64 {
	return di.uid
}

func (di *DataItemImpl) Release() {
	di.dm.Release(di)
}

func (di *DataItemImpl) IsValid() bool {
	return di.raw[0] == DIValid
}

// SetInvalid
// 将di设置为无效，相当于删除这个Di
func (di *DataItemImpl) SetInvalid() {
	di.page.SetDirty(true)
	copy(di.raw[:SzDIValid], []byte{DIInvalid})
}

func (di *DataItemImpl) SetValid() {
	di.page.SetDirty(true)
	copy(di.raw[:SzDIValid], []byte{DIValid})
}

func (di *DataItemImpl) GetPage() Page {
	return di.page
}

//func (di *DataItemImpl) Lock() {
//	di.lock.Lock()
//}
//
//func (di *DataItemImpl) UnLock() {
//	di.lock.Unlock()
//}
//
//func (di *DataItemImpl) RLock() {
//	di.lock.RLock()
//}
//
//func (di *DataItemImpl) RUnLock() {
//	di.lock.RUnlock()
//}

//// BeforeUpdate
//// 事物xid在update dataItem前的操作
//// 记录log
//func (di *DataItemImpl) BeforeUpdate(xid int64, OType OperationType) {
//	// TODO
//}
//
//func (di *DataItemImpl) UndoUpdate(xid int64) {
//	// TODO implement me
//	panic("implement me")
//}
//
//func (di *DataItemImpl) AfterUpdate(xid int64) {
//	//TODO implement me
//	panic("implement me")
//}

// Update 更新DataItem中的数据
// newRaw的长度禁止长于oldRaw, 否则会覆盖Page后续的数据
func (di *DataItemImpl) Update(newRaw []byte) {
	if len(newRaw) > len(di.raw) {
		panic(fmt.Sprintf(
			"Error occurs when updating when updating data item, uid = %d, "+
				"new raw is more longer than old raw", di.uid))
	}
	di.page.SetDirty(true)
	copy(di.raw, newRaw)
}

// WrapDataItemRaw
// RAW: [valid]1[dataSize]8[data]
// 8字节对齐
func WrapDataItemRaw(raw []byte) []byte {
	size := int64(len(raw))
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.BigEndian, DIValid)
	_ = binary.Write(buffer, binary.BigEndian, size)
	_ = binary.Write(buffer, binary.BigEndian, raw)
	// 8字节对齐
	padding := len(buffer.Bytes()) % 8
	if padding != 0 {
		_ = binary.Write(buffer, binary.BigEndian, make([]byte, 8-padding))
	}
	return buffer.Bytes()
}

func SetRawInvalid(raw []byte) []byte {
	copy(raw[:SzDIValid], []byte{DIInvalid})
	return raw
}

func SetRawValid(raw []byte) []byte {
	copy(raw[:SzDIValid], []byte{DIValid})
	return raw
}
