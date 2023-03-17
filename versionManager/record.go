package versionManager

import (
	"bytes"
	"encoding/binary"
	"myDB/dataManager"
)

// Record
// VersionManager向上层模块提供的数据操作的最小单位
// 上层可以将元数据，数据存储在Record中
// Data Format: [ROLLBACK]8[XID]8[Data length]8[DATA]
// DataItem: [valid]1[data length]8[Record Raw Data]
// -> [valid]1[data length]8 [[[ROLLBACK]8[XID]8[Data length]8[DATA]]]
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
	GetPrevious() Record // MVCC 上一个版本的记录, 返回上一条Record Data
	Release()            // 释放Record的引用
	IsWritable() bool
	IsSnapShot() bool
}

type RecordImpl struct {
	di       dataManager.DataItem // [valid]1[length]8[data] --> data:record raw
	raw      []byte
	vm       VersionManager
	uid      int64 // [pageID, offset] of DataItem
	undo     Log
	writable bool // 是否对其具有写权限(快照读没有写权限，当前读具有写权限)
}

// GetData 获取record中的数据
// 对DataItem中的data进行深拷贝
func (record *RecordImpl) GetData() []byte {
	//data := record.getDataItemData()
	//return data[SzRcData+SzRcXid+SzRcRollBack:]
	return record.raw
}

func (record *RecordImpl) GetUid() int64 {
	return record.uid
}

func (record *RecordImpl) GetXid() int64 {
	data := record.raw[SzRcRollBack : SzRcRollBack+SzRcXid]
	return int64(binary.BigEndian.Uint64(data))
}

func (record *RecordImpl) GetPrevious() Record {
	data := record.raw[:SzRcRollBack]
	rollBack := int64(binary.BigEndian.Uint64(data))
	if rollBack == 0 {
		return nil
	} else {
		recordRaw := record.undo.Read(rollBack)
		return DefaultRecordFactory.NewSnapShot(recordRaw, record.undo)
	}
}

func (record *RecordImpl) Release() {
	record.di.Release()
}

func (record *RecordImpl) IsWritable() bool {
	return record.writable
}

func (record *RecordImpl) IsSnapShot() bool {
	return false
}

type SnapShot struct {
	raw  []byte
	undo Log
}

func (s *SnapShot) GetData() []byte {
	return s.raw[SzRcData+SzRcXid+SzRcRollBack:]
}

func (s *SnapShot) GetUid() int64 {
	panic("Error occurs when getting uid of a record, getting uid is not supported on snapshot")
}

func (s *SnapShot) GetXid() int64 {
	data := s.raw[SzRcRollBack : SzRcRollBack+SzRcXid]
	return int64(binary.BigEndian.Uint64(data))
}

func (s *SnapShot) GetPrevious() Record {
	data := s.GetData()[:SzRcRollBack]
	rollBack := int64(binary.BigEndian.Uint64(data))
	if rollBack == 0 {
		return nil
	} else {
		recordRaw := s.undo.Read(rollBack)
		return DefaultRecordFactory.NewSnapShot(recordRaw, s.undo)
	}
}

func (s *SnapShot) Release() {
	panic("Error occurs when releasing a record, releasing is not supported on snapshot")
}

func (s *SnapShot) IsWritable() bool {
	return false
}

func (s *SnapShot) IsSnapShot() bool {
	return true
}

// Record工厂

type RecordFactory interface {
	NewRecord(raw []byte, di dataManager.DataItem, vm VersionManager, uid int64, undo Log, writable bool) Record
	NewSnapShot(raw []byte, undo Log) Record
}

type RecordImplFactory struct{}

func (factory *RecordImplFactory) NewRecord(raw []byte, di dataManager.DataItem, vm VersionManager, uid int64, undo Log, writable bool) Record {
	return &RecordImpl{
		raw: raw, di: di, vm: vm, uid: uid, undo: undo, writable: writable,
	}
}

func (factory *RecordImplFactory) NewSnapShot(raw []byte, undo Log) Record {
	return &SnapShot{
		raw: raw, undo: undo,
	}
}

var DefaultRecordFactory RecordFactory

// WrapRecordRaw
// Data Format: [ROLLBACK]8[XID]8[SIZE]8[DATA]
func WrapRecordRaw(data []byte, xid int64, rollback int64) []byte {
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.BigEndian, rollback)
	_ = binary.Write(buffer, binary.BigEndian, xid)
	_ = binary.Write(buffer, binary.BigEndian, int64(len(data)))
	_ = binary.Write(buffer, binary.BigEndian, data)
	return buffer.Bytes()
}
