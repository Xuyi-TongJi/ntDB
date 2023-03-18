package tableManager

import (
	"bytes"
	"encoding/binary"
	"myDB/indexManager"
)

func init() {
	m := make(map[FieldType]func(any, any) int)
	m[INT32] = func(a any, b any) int {
		ia, ib := a.(int32), b.(int32)
		if ia > ib {
			return 1
		} else if ia == ib {
			return 0
		} else {
			return -1
		}
	}
	m[INT64] = func(a any, b any) int {
		ia, ib := a.(int64), b.(int64)
		if ia > ib {
			return 1
		} else if ia == ib {
			return 0
		} else {
			return -1
		}
	}
	m[STRING] = func(a any, b any) int {
		sa, sb := a.(string), b.(string)
		if sa > sb {
			return 1
		} else if sa == sb {
			return 0
		} else {
			return -1
		}
	}
	DefaultFieldFactory = &FieldImplFactory{
		compareFunctionMap: m,
	}
}

type Field interface {
	GetUid() int64
	IsIndexed() bool
	Range(left any, right any) ([]int64, error)
}

// FieldImpl
// [FieldName][TypeName]8[IndexUid]8
// [FieldName] -> [StringLength]8[StringData]...
// IndexUid 索引根结点
// 如果这个字段没有建立索引，则indexUid = 0

type FieldType int64

const (
	INVALID FieldType = 0
	INT32   FieldType = 1 // [4 Bytes]
	INT64   FieldType = 2 // [8 Bytes]
	STRING  FieldType = 3 // [StringLength]8[StringData]
)

type FieldImpl struct {
	tb        Table
	uid       int64 // 字段元信息存储位置[pageId + offset]
	fieldName string
	fieldType FieldType
	indexUid  int64
	index     indexManager.Index
}

func (f *FieldImpl) GetUid() int64 {
	return f.uid
}

func (f *FieldImpl) IsIndexed() bool {
	return f.indexUid != 0
}

func (f *FieldImpl) Range(left any, right any) ([]int64, error) {
	if f.IsIndexed() {
		return f.index.SearchRange(left, right)
	} else {
		// 全表扫描
		return f.tb.SearchAll()
	}
}

const (
	SzFieldType    int64 = 8
	SzStringLength int64 = 8
	SzIndexUid     int64 = 8
)

type FieldFactory interface {
	NewField(tb Table, uid int64, raw []byte, im indexManager.IndexManager) Field
	GetCompareFunction(fType FieldType) func(any, any) int
}

type FieldImplFactory struct {
	compareFunctionMap map[FieldType]func(any, any) int
}

func (f *FieldImplFactory) NewField(tb Table, uid int64, raw []byte, im indexManager.IndexManager) Field {
	// [FieldName(string_format)][TypeName]8[IndexUid]8
	fieldNameLength := int64(binary.BigEndian.Uint64(raw[:SzStringLength]))
	fieldName := string(raw[SzStringLength : SzStringLength+fieldNameLength])
	fieldType := FieldType(binary.BigEndian.
		Uint64(raw[SzStringLength+fieldNameLength : SzStringLength+fieldNameLength+SzFieldType]))
	indexUid := int64(binary.BigEndian.Uint64(raw[SzStringLength+fieldNameLength+SzFieldType:]))
	var index indexManager.Index
	if indexUid != 0 {
		index = im.LoadIndex(indexUid)
	}
	return &FieldImpl{
		tb:        tb,
		uid:       uid,
		fieldName: fieldName,
		fieldType: fieldType,
		indexUid:  indexUid,
		index:     index,
	}
}

func (f *FieldImplFactory) GetCompareFunction(fType FieldType) func(any, any) int {
	return f.compareFunctionMap[fType]
}

var DefaultFieldFactory FieldFactory

// utils

func WrapFieldRaw(fName string, fType FieldType, indexUid int64) []byte {
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.BigEndian, int64(len(fName)))
	_ = binary.Write(buffer, binary.BigEndian, []byte(fName))
	_ = binary.Write(buffer, binary.BigEndian, int64(fType))
	_ = binary.Write(buffer, binary.BigEndian, indexUid)
	return buffer.Bytes()
}

type ErrorInvalidFType struct{}

func (err *ErrorInvalidFType) Error() string {
	return "Invalid Field Type"
}

func TransToFieldType(typeStr string) (FieldType, error) {
	switch typeStr {
	case "int32":
		return INT32, nil
	case "int64":
		return INT64, nil
	case "string":
		return STRING, nil
	default:
		return INVALID, &ErrorInvalidFType{}
	}
}
