package tableManager

import (
	"bytes"
	"encoding/binary"
)

// Row 普通用户记录，索引/真实记录
// data format of Row
// [RowType]8[NextRowUid]8[Field1Value]...[FieldNValue]
// NextRowUid == 0 if this is the last row

func init() {
	DefaultRowFactory = &RowImplFactory{}
}

type Row interface {
	GetRType() RowType
	GetNextUid() int64
	GetValues() []any
}

type RowImpl struct {
	rType   RowType
	nextUid int64
	values  []any
}

const (
	SzRowType    int64 = 8
	SzNextRowUid int64 = 8
)

func (r *RowImpl) GetRType() RowType {
	return r.rType
}

func (r *RowImpl) GetNextUid() int64 {
	return r.nextUid
}

func (r *RowImpl) GetValues() []any {
	return r.values
}

type RowType int64

const (
	INDEX  RowType = 0
	RECORD RowType = 1
)

type RowFactory interface {
	NewRow(tb Table, raw []byte) Row
}

var DefaultRowFactory RowFactory

type RowImplFactory struct{}

func (r *RowImplFactory) NewRow(tb Table, raw []byte) Row {
	fields := tb.GetFields()
	values := make([]any, len(fields))
	offset := int64(0)
	rType := int64(binary.BigEndian.Uint64(raw[offset : offset+SzRowType]))
	offset += SzRowType
	nextUid := int64(binary.BigEndian.Uint64(raw[offset : offset+SzNextRowUid]))
	for i, field := range fields {
		fType := field.GetFType()
		length := FTypeLength[fType]
		if length == VARIABLE {
			// 变长字段
			length = int64(binary.BigEndian.Uint64(raw[offset : offset+SzVariableLength]))
			offset += SzVariableLength
		}
		value, err := getFTypeValue(fType, raw[offset:offset+length])
		if err != nil {
			panic(err)
		}
		offset += length
		values[i] = value
	}
	return &RowImpl{
		rType:   RowType(rType),
		nextUid: nextUid,
		values:  values,
	}
}

func WrapRowRaw(tb Table, rType RowType, nextRowUid int64, values []any) ([]byte, error) {
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.BigEndian, rType)
	_ = binary.Write(buffer, binary.BigEndian, nextRowUid)
	cnt := len(values)
	fields := tb.GetFields()
	for i := 0; i < cnt; i++ {
		if bytes, err := fieldValueToBytes(fields[i].GetFType(), values[i]); err != nil {
			return nil, err
		} else {
			_ = binary.Write(buffer, binary.BigEndian, bytes)
		}
	}
	return buffer.Bytes(), nil
}
