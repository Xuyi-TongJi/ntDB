package dataManager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

// DataItem
// DataManager 向上层提供的抽象, 上层模块调用
// 上层模块通过地址信息向DataManager请求得到DataItem
// DataItem和Page类似于Golang MMU中的mspan和page的关系
type DataItem interface {
	GetData() []byte
	GetDataLength() int64
	GetRaw() []byte // 深拷贝，确保上层模块不会直接修改dataItem中的数据

	IsValid() bool
	SetInvalid()
	SetValid()
	GetPage() Page
	GetUid() int64
	Release()
	Update(newRaw []byte)
}

// DataItemImpl
// RAW: [valid]1[dataSize]8[data]
// raw是Page[offset:offset+raw_length]的一段切片，修改raw将直接修改page上的数据
// 因为上层（VM）确保了事物之间的隔离性即，两个事物不会并发操作同一个DataItem, 因此对dataItem的修改操作不用加锁
// 且DataManager的PageCtl模块确保了在添加(Append)操作时不会同时操作一个DataItem（uid）
// **** 将数据返回给上层时，必须深拷贝，禁止上层对Page中的数据进行修改
type DataItemImpl struct {
	page Page // BufferPool中的页, 当前raw位于哪个页
	uid  int64
	dm   DataManager
	raw  []byte
}

const (
	DIInvalid    byte  = 0
	DIValid      byte  = 1
	SzDIValid    int64 = 1
	SzDIDataSize int64 = 8
)

func NewDataItem(raw []byte, dm DataManager,
	page Page, uid int64) DataItem {
	return &DataItemImpl{
		page: page,
		uid:  uid,
		dm:   dm,
		raw:  raw,
	}
}

// GetData
// 获得DataItem中的载荷DATA
// 深拷贝
// [valid]1[Length]8[DATA]... -> [DATA]
func (di *DataItemImpl) GetData() []byte {
	length := di.GetDataLength()
	data := di.raw[SzDIValid+SzDIDataSize : SzDIValid+SzDIDataSize+length]
	copyData := make([]byte, len(data))
	copy(copyData, data)
	return copyData
}

func (di *DataItemImpl) GetDataLength() int64 {
	length := di.raw[SzDIValid : SzDIValid+SzDIDataSize]
	return int64(binary.BigEndian.Uint64(length))
}

// GetRaw
// 获得DataItem Raw [Valid]1[Length]8[Data]
// 深拷贝
func (di *DataItemImpl) GetRaw() []byte {
	copyData := make([]byte, len(di.raw))
	copy(copyData, di.raw)
	return copyData
}

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
// 可以无锁，VM可以确保其数据安全
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
func WrapDataItemRaw(data []byte) []byte {
	size := int64(len(data))
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.BigEndian, DIValid)
	_ = binary.Write(buffer, binary.BigEndian, size)
	_ = binary.Write(buffer, binary.BigEndian, data)
	log.Printf("[DATA ITEM LINE 134] WRAP DATAITEM, size = %d\n", size)
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
