package versionManager

import (
	"bytes"
	"encoding/binary"
	"myDB/dataManager"
)

// Record
// VersionManager向上层模块提供的数据操作的最小单位
// 上层可以将元数据，数据存储在Record中
// Data Format: [Valid]1[ROLLBACK]8[XID]8[Data length]8[DATA]
// DataItem: [valid]1[data length]8[Record Raw Data]
// -> [valid]1[data length]8 [[Valid]1[[ROLLBACK]8[XID]8[DATA]]]
// ROLLBACK == 0 -> then this is the first record version

const (
	RCInvalid    byte  = 0
	RCValid      byte  = 1
	SzValid      int64 = 1
	SzRcXid      int64 = 8
	SzRcRollBack int64 = 8
)

func init() {
	DefaultRecordFactory = &RecordImplFactory{}
}

type Record interface {
	IsValid() bool
	GetRaw() []byte
	GetData() []byte
	GetUid() int64 // uid record所在dataItem的pageId和offset
	GetXid() int64
	GetPrevious() Record // MVCC 上一个版本的记录, 返回上一条Record Data
	IsSnapShot() bool
}

type RecordImpl struct {
	valid    bool
	rollback int64
	xid      int64
	data     []byte
	undo     Log
	vm       VersionManager

	di  dataManager.DataItem // [valid]1[length]8[data] --> data:record raw
	raw []byte               // raw是DataItem中的DATA段的深拷贝，直接修改raw不会修改DataItem中的数据
	uid int64                // [pageID, offset] of DataItem
}

func (record *RecordImpl) IsValid() bool {
	return record.valid
}

// GetRaw 获取record中的数据
// 对DataItem中的data进行深拷贝
func (record *RecordImpl) GetRaw() []byte {
	return record.raw
}

// GetData 获取record中的数据
// 对DataItem中的data进行深拷贝
func (record *RecordImpl) GetData() []byte {
	return record.data
}

func (record *RecordImpl) GetUid() int64 {
	return record.uid
}

func (record *RecordImpl) GetXid() int64 {
	return record.xid
}

func (record *RecordImpl) GetPrevious() Record {
	if record.rollback == 0 {
		return nil
	} else {
		recordRaw := record.undo.Read(record.rollback)
		return DefaultRecordFactory.NewSnapShot(recordRaw, record.undo)
	}
}

//func (record *RecordImpl) Release() {
//	record.di.Release()
//}

func (record *RecordImpl) IsSnapShot() bool {
	return false
}

type SnapShot struct {
	valid    bool
	xid      int64
	rollback int64
	data     []byte
	raw      []byte
	undo     Log
}

func (s *SnapShot) IsValid() bool {
	return s.valid
}

func (s *SnapShot) GetRaw() []byte {
	return s.raw
}

func (s *SnapShot) GetData() []byte {
	return s.data
}

func (s *SnapShot) GetUid() int64 {
	panic("Error occurs when getting uid of a record, getting uid is not supported on snapshot")
}

func (s *SnapShot) GetXid() int64 {
	return s.xid
}

func (s *SnapShot) GetPrevious() Record {
	if s.rollback == 0 {
		return nil
	} else {
		recordRaw := s.undo.Read(s.rollback)
		return DefaultRecordFactory.NewSnapShot(recordRaw, s.undo)
	}
}

//func (s *SnapShot) Release() {
//	panic("Error occurs when releasing a record, releasing is not supported on snapshot")
//}

func (s *SnapShot) IsSnapShot() bool {
	return true
}

// Record工厂

type RecordFactory interface {
	NewRecord(raw []byte, di dataManager.DataItem, vm VersionManager, uid int64, undo Log) Record
	NewSnapShot(raw []byte, undo Log) Record
}

type RecordImplFactory struct{}

func (factory *RecordImplFactory) NewRecord(raw []byte, di dataManager.DataItem, vm VersionManager, uid int64, undo Log) Record {
	valid := raw[0] == RCValid
	offset := SzValid
	rollback := int64(binary.LittleEndian.Uint64(raw[offset : offset+SzRcRollBack]))
	offset += SzRcRollBack
	xid := int64(binary.LittleEndian.Uint64(raw[offset : offset+SzRcXid]))
	offset += SzRcXid
	data := raw[offset:]
	return &RecordImpl{
		valid:    valid,
		rollback: rollback,
		xid:      xid,
		data:     data,
		raw:      raw, di: di, vm: vm, uid: uid, undo: undo,
	}
}

func (factory *RecordImplFactory) NewSnapShot(raw []byte, undo Log) Record {
	valid := raw[0] == RCValid
	offset := SzValid
	rollback := int64(binary.LittleEndian.Uint64(raw[offset : offset+SzRcRollBack]))
	offset += SzRcRollBack
	xid := int64(binary.LittleEndian.Uint64(raw[offset : offset+SzRcXid]))
	offset += SzRcXid
	data := raw[offset:]
	return &SnapShot{
		valid:    valid,
		rollback: rollback,
		xid:      xid,
		data:     data,
		raw:      raw, undo: undo,
	}
}

var DefaultRecordFactory RecordFactory

// WrapRecordRaw
// Data Format: [ROLLBACK]8[XID]8[SIZE]8[DATA]
func WrapRecordRaw(valid bool, data []byte, xid int64, rollback int64) []byte {
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.LittleEndian, valid)
	_ = binary.Write(buffer, binary.LittleEndian, rollback)
	_ = binary.Write(buffer, binary.LittleEndian, xid)
	_ = binary.Write(buffer, binary.LittleEndian, int64(len(data)))
	_ = binary.Write(buffer, binary.LittleEndian, data)
	return buffer.Bytes()
}
