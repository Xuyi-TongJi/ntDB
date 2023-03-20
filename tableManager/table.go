package tableManager

import (
	"bytes"
	"encoding/binary"
)

func init() {
	DefaultTableFactory = &TableImplFactory{}
}

// Table

type Table interface {
	GetName() string
	GetNextUid() int64
	GetUid() int64               // 表的uid(存储位置)
	SearchAll() ([]int64, error) // 全表扫描
}

// DB中的所有表组织成链表的形式
// Data format of Table metadata
// [TABLE_MASK]4[TableName(string_format)][nextTable_uid]8[TableFieldNumber]4
// [Field1UID]8[Field2UID]8...[FieldNUID]8
// [FIRST_RECORD_UID] 8 (v1.0 为实现索引)
// v1.0 first_record_uid 该表的第一个数据的uid
// nextTable_uid == 0 if this is the last table
// first_record_uid == 0 if this is an empty table

const (
	SzTableUid         int64 = 8
	SzFieldUid         int64 = 8
	SzTableFieldNumber int64 = 4
)

type TableImpl struct {
	tm             TableManager
	uid            int64 // the uid where the metadata of table stores
	tableName      string
	status         TableStatus
	nextUid        int64 // = 0 if this is the last table
	fields         []Field
	firstRecordUid int64
}

func (tb *TableImpl) GetName() string {
	return tb.tableName
}

func (tb *TableImpl) GetUid() int64 {
	return tb.uid
}

func (tb *TableImpl) GetNextUid() int64 {
	return tb.nextUid
}

func (tb *TableImpl) SearchAll() ([]int64, error) {
	// TODO
	return nil, nil
}

type TableStatus byte

const (
	ACTIVE    TableStatus = 1
	TableMask int32       = 0x3f3f3f3f
)

// WrapTableRaw
// 包装一个空表的Raw
func WrapTableRaw(tableName string, nextUid int64, fields []Field) []byte {
	buffer := bytes.NewBuffer([]byte{})
	stringLength := int64(len(tableName))
	_ = binary.Write(buffer, binary.BigEndian, stringLength)
	_ = binary.Write(buffer, binary.BigEndian, []byte(tableName))
	_ = binary.Write(buffer, binary.BigEndian, nextUid)
	fieldLength := int32(len(fields))
	_ = binary.Write(buffer, binary.BigEndian, fieldLength)
	for i := int32(0); i < fieldLength; i++ {
		uid := fields[i].GetUid()
		_ = binary.Write(buffer, binary.BigEndian, uid)
	}
	_ = binary.Write(buffer, binary.BigEndian, int64(0))
	return buffer.Bytes()
}

type TableFactory interface {
	NewTable(uid int64, raw []byte, tm TableManager) Table
}

type TableImplFactory struct{}

// NewTable
// 当raw不是一个有效的Table字段时，panic
func (f *TableImplFactory) NewTable(uid int64, raw []byte, tm TableManager) Table {
	// check if this is a table
	mask := int32(binary.BigEndian.Uint32(raw[:SzMask]))
	if mask != TableMask {
		panic("Error occurs when creating a table struct, it is not a valid table raw")
	}
	raw = append(raw[SzMask:])
	tableNameLength := int64(binary.BigEndian.Uint64(raw[:SzStringLength]))
	tableName := string(raw[SzStringLength : SzStringLength+tableNameLength])
	nextUid := int64(binary.BigEndian.Uint64(
		raw[SzStringLength+tableNameLength : SzStringLength+tableNameLength+SzTableUid]))
	tableFieldNumber := int32(binary.BigEndian.Uint32(
		raw[SzStringLength+tableNameLength+SzTableUid : SzStringLength+tableNameLength+SzTableUid+SzTableFieldNumber]))
	fields := make([]Field, tableFieldNumber)
	table := &TableImpl{
		tm:             tm,
		uid:            uid,
		tableName:      tableName,
		status:         ACTIVE,
		nextUid:        nextUid,
		fields:         fields,
		firstRecordUid: 0,
	}
	pointer := SzStringLength + tableNameLength + SzTableUid + SzTableFieldNumber
	for i := int32(0); i < tableFieldNumber; i++ {
		fUid := int64(binary.BigEndian.Uint64(raw[pointer : pointer+SzFieldUid]))
		pointer += SzFieldUid
		fields[i] = tm.LoadField(table, fUid)
	}
	table.firstRecordUid = int64(binary.BigEndian.Uint64(raw[pointer:]))
	return table
}

var DefaultTableFactory TableFactory
