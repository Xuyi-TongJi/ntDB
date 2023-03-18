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
	GetUid() int64
	SearchAll() ([]int64, error)
}

// 链表
// Data format of Table metadata
// [TableName(string_format)][nextTable_uid]8[TableFieldNumber]4
// [Field1UID]8[Field2UID]8...[FieldNUID]8
// nextTable_uid == 0 if this is the last table

const (
	SzTableUid         int64 = 8
	SzFieldUid         int64 = 8
	SzTableFieldNumber int64 = 4
)

type TableImpl struct {
	tm        TableManager
	uid       int64 // the uid where the metadata of table stores
	tableName string
	status    TableStatus
	nextUid   int64 // = 0 if this is the last table
	fields    []Field
}

func (tb *TableImpl) GetUid() int64 {
	return tb.uid
}

func (tb *TableImpl) SearchAll() ([]int64, error) {
	// TODO
	return nil, nil
}

type TableStatus byte

const (
	ACTIVE TableStatus = 1
)

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
	return buffer.Bytes()
}

type TableFactory interface {
	NewTable(uid int64, raw []byte, tm TableManager) Table
}

type TableImplFactory struct{}

func (f *TableImplFactory) NewTable(uid int64, raw []byte, tm TableManager) Table {
	tableNameLength := int64(binary.BigEndian.Uint64(raw[:SzStringLength]))
	tableName := string(raw[SzStringLength : SzStringLength+tableNameLength])
	nextUid := int64(binary.BigEndian.Uint64(
		raw[SzStringLength+tableNameLength : SzStringLength+tableNameLength+SzTableUid]))
	tableFieldNumber := int32(binary.BigEndian.Uint32(
		raw[SzStringLength+tableNameLength+SzTableUid : SzStringLength+tableNameLength+SzTableUid+SzTableFieldNumber]))
	fields := make([]Field, tableFieldNumber)
	table := &TableImpl{
		tm:        tm,
		uid:       uid,
		tableName: tableName,
		status:    ACTIVE,
		nextUid:   nextUid,
		fields:    fields,
	}
	pointer := SzStringLength + tableNameLength + SzTableUid + SzTableFieldNumber
	for i := int32(0); i < tableFieldNumber; i++ {
		fUid := int64(binary.BigEndian.Uint64(raw[pointer : pointer+SzFieldUid]))
		pointer += SzFieldUid
		fields[i] = tm.LoadField(table, fUid)
	}
	return table
}

var DefaultTableFactory TableFactory
