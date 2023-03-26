package tableManager

import (
	"bytes"
	"encoding/binary"
	"myDB/indexManager"
	"strconv"
	"strings"
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
	FTypeLength = map[FieldType]int64{}
	FTypeLength[INT32] = 4
	FTypeLength[INT64] = 8
	FTypeLength[STRING] = VARIABLE // 变长字段
}

type Field interface {
	GetUid() int64
	GetFType() FieldType
	GetName() string
	SetTable(tb Table)
	IsIndexed() bool
}

// FieldImpl
// [FieldMask]4[FieldName][TypeName]8[IndexUid]8
// [FieldName] -> [StringLength]8[StringData]...
// IndexUid 索引根结点
// Field的raw和table信息无关
// 如果这个字段没有建立索引，则indexUid = 0

type FieldType int64

// FTypeLength 字段对应的字段长度, 如果是变长字段，则对应的存储结构为[length]8[data], 该值为-1
var FTypeLength map[FieldType]int64

const (
	VARIABLE  int64     = -1
	FieldMask int32     = 0xf3f3f3
	INVALID   FieldType = 0
	INT32     FieldType = 1 // [4 Bytes]
	INT64     FieldType = 2 // [8 Bytes]
	STRING    FieldType = 3 // [StringLength]8[StringData]
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

func (f *FieldImpl) GetName() string {
	return f.fieldName
}

func (f *FieldImpl) GetFType() FieldType {
	return f.fieldType
}

func (f *FieldImpl) SetTable(tb Table) {
	f.tb = tb
}

func (f *FieldImpl) IsIndexed() bool {
	return f.indexUid != 0
}

const (
	SzFieldType      int64 = 8
	SzVariableLength int64 = 8
	SzIndexUid       int64 = 8
)

type FieldFactory interface {
	NewField(tb Table, uid int64, raw []byte, im indexManager.IndexManager) Field
	GetCompareFunction(fType FieldType) func(any, any) int
}

type FieldImplFactory struct {
	compareFunctionMap map[FieldType]func(any, any) int
}

// NewField
// 工厂方法
// 当Field不是一个有效的Field字段时，panic
func (f *FieldImplFactory) NewField(tb Table, uid int64, raw []byte, im indexManager.IndexManager) Field {
	mask := int32(binary.BigEndian.Uint32(raw[:SzMask]))
	if mask != FieldMask {
		panic("Error occurs when creating a field struct, it is not a valid field raw")
	}
	raw = raw[SzMask:]
	// [FieldName(string_format)][TypeName]8[IndexUid]8
	fieldNameLength := int64(binary.BigEndian.Uint64(raw[:SzVariableLength]))
	fieldName := string(raw[SzVariableLength : SzVariableLength+fieldNameLength])
	fieldType := FieldType(binary.BigEndian.
		Uint64(raw[SzVariableLength+fieldNameLength : SzVariableLength+fieldNameLength+SzFieldType]))
	indexUid := int64(binary.BigEndian.Uint64(raw[SzVariableLength+fieldNameLength+SzFieldType:]))
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
	_ = binary.Write(buffer, binary.BigEndian, FieldMask)
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
	typeStr = strings.ToUpper(typeStr)
	switch typeStr {
	case "INT32":
		return INT32, nil
	case "INT64":
		return INT64, nil
	case "STRING":
		return STRING, nil
	default:
		return INVALID, &ErrorInvalidFType{}
	}
}

func TransIndexed(indexed string) (bool, error) {
	indexed = strings.ToUpper(indexed)
	switch indexed {
	case "INDEXED":
		return true, nil
	case "":
		return false, nil
	}
	return false, &ErrorUnsupportedOperationType{}
}

type ErrorFTypeInvalid struct{}

func (err *ErrorFTypeInvalid) Error() string {
	return "The type value bytes can not be traversed to the target field type"
}

func getFTypeValue(fType FieldType, raw []byte) (any, error) {
	switch fType {
	case INT32:
		{
			return int32(binary.BigEndian.Uint32(raw)), nil
		}
	case INT64:
		{
			return int64(binary.BigEndian.Uint64(raw)), nil
		}
	case STRING:
		{
			return string(raw), nil
		}
	default:
		return nil, &ErrorFTypeInvalid{}
	}
}

type ErrorValueNotMatch struct{}

func (err *ErrorValueNotMatch) Error() string {
	return "Field Value doesn't match with the field type"
}

func fieldValueToBytes(fType FieldType, value any) ([]byte, error) {
	buffer := bytes.NewBuffer([]byte{})
	switch fType {
	case INT32:
		{
			if v, available := value.(int32); !available {
				return nil, &ErrorValueNotMatch{}
			} else {
				_ = binary.Write(buffer, binary.BigEndian, v)
			}
		}
	case INT64:
		{
			if v, available := value.(int64); !available {
				return nil, &ErrorValueNotMatch{}
			} else {
				_ = binary.Write(buffer, binary.BigEndian, v)
			}
		}
	case STRING:
		{
			if s, available := value.(string); !available {
				return nil, &ErrorValueNotMatch{}
			} else {
				_ = binary.Write(buffer, binary.BigEndian, int64(len(s)))
				_ = binary.Write(buffer, binary.BigEndian, []byte(s))
			}
		}
	}
	return buffer.Bytes(), nil
}

func traverseStringToValue(fType FieldType, value string) (any, error) {
	switch fType {
	case INT32:
		{
			ret, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return nil, &ErrorValueNotMatch{}
			}
			return int32(ret), nil
		}
	case INT64:
		{
			ret, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, &ErrorValueNotMatch{}
			}
			return ret, nil
		}
	case STRING:
		{
			return value, nil
		}
	}
	return nil, &ErrorFTypeInvalid{}
}
