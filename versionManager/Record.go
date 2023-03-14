package versionManager

import (
	"bytes"
	"encoding/binary"
	"myDB/dataManager"
)

// Record
// VersionManager向上层模块提供的数据操作的最小单位
// Data Format: [ROLLBACK]8[XID]8[SIZE]8[DATA]
// ROLLBACK == 0 -> then this is the first record version

const (
	SzRcData     int64 = 8
	SzRcXid      int64 = 8
	SzRcRollBack int64 = 8
)

func init() {
	DefaultRecordFactory = &RecordImplFactory{}
}

type Record interface {
	GetData() []byte
	GetUid() int64 // uid record所在dataItem的pageId和offset
	GetXid() int64
	GetPrevious() []byte // MVCC 上一个版本的记录, 返回上一条Record Data
}

type RecordImpl struct {
	di   dataManager.DataItem // [valid]1[length]8[data] --> data:record
	vm   VersionManager
	uid  int64 // [pageID, offset] of DataItem
	undo Log
}

// GetData 获取record中的数据
// 对DataItem中的data进行深拷贝
func (record *RecordImpl) GetData() []byte {
	data := record.getDataItemData()
	return data[SzRcData+SzRcXid+SzRcRollBack:]
}

func (record *RecordImpl) GetUid() int64 {
	return record.uid
}

func (record *RecordImpl) GetXid() int64 {
	data := record.getDataItemData()[SzRcRollBack : SzRcRollBack+SzRcXid]
	return int64(binary.BigEndian.Uint64(data))
}

func (record *RecordImpl) GetPrevious() []byte {
	data := record.getDataItemData()[:SzRcRollBack]
	rollBack := int64(binary.BigEndian.Uint64(data))
	if rollBack == 0 {
		return nil
	} else {
		return record.undo.Read(rollBack)
	}
}

// getDataItemData
// 对DataItem中的data进行深拷贝
func (record *RecordImpl) getDataItemData() []byte {
	record.di.RLock()
	defer record.di.RUnLock()
	data := record.di.GetData()
	copyData := make([]byte, len(data))
	copy(copyData, data)
	return copyData
}

type RecordFactory interface {
	NewRecord(di dataManager.DataItem, vm VersionManager, uid int64, undo Log) Record
}

type RecordImplFactory struct{}

func (factory *RecordImplFactory) NewRecord(di dataManager.DataItem, vm VersionManager, uid int64, undo Log) Record {
	return &RecordImpl{
		di: di, vm: vm, uid: uid, undo: undo,
	}
}

var DefaultRecordFactory RecordFactory

func wrapRecordRaw(data []byte, xid int64, previous int64) []byte {
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.BigEndian, xid)
	data = append(data, buffer.Bytes()...)
	return data
}

func GetXidByRecordData(raw []byte) int64 {
	buf := raw[SzRcRollBack : SzRcRollBack+SzRcXid]
	return int64(binary.BigEndian.Uint64(buf))
}
