package tableManager

import (
	"bytes"
	"encoding/binary"
)

// Row 普通用户记录，索引/真实记录
// data format of Row 双向链表
// 真实记录
// [RowType]8[PrevRowUid]8[NextRowUid]8[Field1Value]...[FieldNValue]
// NextRowUid == 0 if this is the last row
// PrevRowUid == 0

// 索引记录
// TODO

func init() {
	DefaultRowFactory = &RowImplFactory{}
}

type Row interface {
	GetUid() int64
	GetRType() RowType
	GetNextUid() int64
	GetPrevUid() int64
	GetValues() []any
}

type RowImpl struct {
	uid     int64
	rType   RowType
	prevUid int64
	nextUid int64
	values  []any
}

const (
	SzRowType int64 = 8
	SzRowUid  int64 = 8
)

func (r *RowImpl) GetUid() int64 {
	return r.uid
}

func (r *RowImpl) GetRType() RowType {
	return r.rType
}

func (r *RowImpl) GetNextUid() int64 {
	return r.nextUid
}

func (r *RowImpl) GetPrevUid() int64 {
	return r.prevUid
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
	NewRow(uid int64, tb Table, raw []byte) Row
}

var DefaultRowFactory RowFactory

type RowImplFactory struct{}

func (r *RowImplFactory) NewRow(uid int64, tb Table, raw []byte) Row {
	fields := tb.GetFields()
	values := make([]any, len(fields))
	offset := int64(0)
	rType := int64(binary.LittleEndian.Uint64(raw[offset : offset+SzRowType]))
	offset += SzRowType
	prevUid := int64(binary.LittleEndian.Uint64(raw[offset : offset+SzRowUid]))
	offset += SzRowUid
	nextUid := int64(binary.LittleEndian.Uint64(raw[offset : offset+SzRowUid]))
	offset += SzRowUid
	for i, field := range fields {
		fType := field.GetFType()
		length := FTypeLength[fType]
		if length == VARIABLE {
			// 变长字段
			length = int64(binary.LittleEndian.Uint64(raw[offset : offset+SzVariableLength]))
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
		uid:     uid,
		rType:   RowType(rType),
		prevUid: prevUid,
		nextUid: nextUid,
		values:  values,
	}
}

func WrapRowRaw(tb Table, rType RowType, prevRowUid, nextRowUid int64, values []any) ([]byte, error) {
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.LittleEndian, rType)
	_ = binary.Write(buffer, binary.LittleEndian, prevRowUid)
	_ = binary.Write(buffer, binary.LittleEndian, nextRowUid)
	cnt := len(values)
	fields := tb.GetFields()
	for i := 0; i < cnt; i++ {
		if bytes, err := fieldValueToBytes(fields[i].GetFType(), values[i]); err != nil {
			return nil, err
		} else {
			_ = binary.Write(buffer, binary.LittleEndian, bytes)
		}
	}
	return buffer.Bytes(), nil
}
