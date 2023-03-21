package tableManager

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"myDB/config"
	"myDB/indexManager"
	"myDB/transactions"
	"myDB/versionManager"
	"os"
	"sync"
)

// TableManager
// 用户模块(Connection)直接调用
// 表和字段管理

type TableManager interface {
	Begin() int64 // 开启一个事物，返回xid
	Commit(xid int64) error
	Abort(xid int64) error

	Show(xid int64) ([]*ResponseObject, error) // 展示DB中的所有表
	Create(xid int64, create Create) error     // create table

	Insert(xid int64, insert Insert) error
	Read(xid int64, sel Select) ([]*ResponseObject, error)      // select
	Update(xid int64, update Update) ([]*ResponseObject, error) // update fields
	Delete(xid int64, delete Delete) ([]*ResponseObject, error)

	LoadField(tb Table, uid int64) Field
	CreateField(xid int64, fieldName string, fieldType FieldType, indexed bool) (Field, error)

	LoadTable(uid int64) Table
	CreateTable(xid int64, tableName string, fields []*FieldCreate) (Table, error)
}

const (
	TMP         string = "tmp"
	SzMask      int64  = 4 // 字段类型Mask
	bootFileSuf string = ".boot"
)

type TMImpl struct {
	vm          versionManager.VersionManager
	im          indexManager.IndexManager // 保留字段, 实现索引后使用
	tables      map[string]int64          // name -> uid
	tableUid    map[int64]string          // uid -> name
	bootFile    *os.File                  // bootFile里保存一个八字节长度tbUid, 为tb链表的头部，在每次数据库启动时，通过这个文件初始化tableUid
	topTableUid int64
	path        string
	lock        *sync.RWMutex // 保护tables和tableUid(CreateTable和Show)
}

// error

type ErrorTableNotExist struct{}
type ErrorInvalidFieldCount struct{}
type ErrorInvalidFieldName struct{}

func (err *ErrorTableNotExist) Error() string {
	return "Table doesn't exist"
}

func (err *ErrorInvalidFieldCount) Error() string {
	return "Field count to be inserted doesn't match with the table"
}

func (err *ErrorInvalidFieldName) Error() string {
	return "The field doesn't exist in this table"
}

func (tm *TMImpl) Begin() int64 {
	return tm.vm.Begin(config.IsolationLevel)
}

func (tm *TMImpl) Commit(xid int64) error {
	tm.vm.Commit(xid)
	return nil
}

func (tm *TMImpl) Abort(xid int64) error {
	tm.vm.Abort(xid)
	return nil
}

// Show
// 展示DB中的所有表
func (tm *TMImpl) Show(xid int64) ([]*ResponseObject, error) {
	// read快照读 所有UID， 不能直接使用tables, 因为会有版本问题
	// title
	res := make([]*ResponseObject, 0)
	res = append(res, &ResponseObject{"tableName", 0, 0})
	rowId := 1
	tm.lock.RLock()
	defer tm.lock.RUnlock()
	for uid, _ := range tm.tableUid {
		if record := tm.vm.Read(xid, uid); record != nil {
			tabName := tm.tableUid[uid]
			res = append(res, &ResponseObject{tabName, rowId, 0})
			rowId += 1
		}
	}
	return res, nil
}

func (tm *TMImpl) Create(xid int64, create Create) error {
	_, err := tm.CreateTable(xid, create.tbName, create.fields)
	return err
}

func (tm *TMImpl) Insert(xid int64, insert Insert) error {
	// check valid
	if uid, err := tm.getTbUid(insert.tbName); err != nil {
		return err
	} else {
		record := tm.vm.Read(xid, uid)
		if record == nil {
			return &ErrorTableNotExist{}
		}
		tb := DefaultTableFactory.NewTable(uid, record.GetData(), tm)
		// 该表对当前事物可见
		// check valid
		insertValueCount := len(insert.values)
		if insertValueCount != len(tb.GetFields())-1 {
			return &ErrorInvalidFieldCount{}
		}
		// TODO INDEX
		if raw, err := WrapRowRaw(tb, RECORD, tb.GetFirstRecordUid(), insert.values); err != nil {
			return err
		} else {
			uid, err := tm.vm.Insert(xid, raw, tb.GetUid())
			if err != nil {
				return err
			}
			newTableRaw := WrapTableRaw(insert.tbName, tb.GetNextUid(), tb.GetFields(), uid)
			newUid, err := tm.vm.Update(xid, tb.GetUid(), tb.GetUid(), newTableRaw)
			// **** newUid must equal to the current one
			// assert
			if newUid != uid {
				panic("Error occurs when updating table meta data")
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// Read
// Select请求读取数据
func (tm *TMImpl) Read(xid int64, sel Select) ([]*ResponseObject, error) {
	uid, err := tm.getTbUid(sel.tbName)
	if err != nil {
		return nil, err
	}
	if record := tm.vm.Read(xid, uid); record == nil {
		return nil, &ErrorTableNotExist{}
	} else {
		tb := DefaultTableFactory.NewTable(uid, record.GetData(), tm)
		// check where condition valid
		if sel.where != nil && sel.where.compare != nil {
			fieldName := sel.where.compare.fieldName
			find := false
			for _, field := range tb.GetFields() {
				if field.GetName() == fieldName {
					find = true
					if err := checkValueMatch(field.GetFType(), sel.where.compare.value); err != nil {
						return nil, err
					}
					break
				}
			}
			if !find {
				return nil, &ErrorInvalidFieldName{}
			}
		}
		// read data
		rows, err := tm.searchAll(xid, tb)
		if err != nil {
			return nil, err
		}
		targetFieldsId := make([]int, 0)
		for i, field := range tb.GetFields() {
			for _, fN := range sel.fNames {
				if field.GetName() == fN {
					targetFieldsId = append(targetFieldsId, i)
					break
				}
			}
		}
		rowCnt := 1
		// title
		response := tm.wrapTableResponseTitle(sel.fNames)
		// addResponse
		for _, row := range rows {
			if matchWhereCondition(row, tb, sel.where) {
				colCnt := 0
				for _, target := range targetFieldsId {
					// row.GetValues[target]
					value, _ := fieldValueToBytes(tb.GetFields()[target].GetFType(), row.GetValues()[target])
					response = append(response, &ResponseObject{
						payload: string(value),
						rowId:   rowCnt,
						colId:   colCnt,
					})
					colCnt += 1
				}
				rowCnt += 1
			}
		}
		return response, nil
	}
}

func (tm *TMImpl) Update(xid int64, update Update) ([]*ResponseObject, error) {
	//TODO implement me
	panic("implement me")
}

func (tm *TMImpl) Delete(xid int64, delete Delete) ([]*ResponseObject, error) {
	//TODO implement me
	panic("implement me")
}

func (tm *TMImpl) LoadField(tb Table, uid int64) Field {
	record := tm.vm.Read(transactions.SuperXID, uid)
	if record == nil {
		panic(fmt.Sprintf("Error occurs when loading metadata of field"))
	}
	// RECORD
	// [ROLLBACK]8[XID]8[Data length]8[DATA]
	data := record.GetData()
	// [DATA] -> of field meta data
	return DefaultFieldFactory.NewField(tb, uid, data, tm.im)
}

func (tm *TMImpl) CreateField(xid int64, fieldName string, fieldType FieldType, indexed bool) (Field, error) {
	var indexUid int64 = 0
	if indexed {
		// TODO index
		indexUid = tm.im.CreateIndex(xid, DefaultFieldFactory.GetCompareFunction(fieldType))
	}
	raw := WrapFieldRaw(fieldName, fieldType, indexUid) // format RECORD DATA
	// insert metadata to dm
	if uid, err := tm.vm.Insert(xid, raw, versionManager.MetaDataTbUid); err != nil {
		return nil, err
	} else {
		return DefaultFieldFactory.NewField(nil, uid, raw, tm.im), err // table字段需要后续添加
	}
}

func (tm *TMImpl) LoadTable(uid int64) Table {
	record := tm.vm.Read(transactions.SuperXID, uid)
	if record == nil {
		panic(fmt.Sprintf("Error occurs when loading metadata of table"))
	}
	data := record.GetData()
	return DefaultTableFactory.NewTable(uid, data, tm)
}

// CreateTable
// 创建表
// 元数据, 不需要获取表锁
// 创建表时，需要创建所有的Field
func (tm *TMImpl) CreateTable(xid int64, tableName string, fields []*FieldCreate) (Table, error) {
	// CreateField
	fs := make([]Field, len(fields))
	for i, fc := range fields {
		field, err := tm.CreateField(xid, fc.fName, fc.fType, fc.indexed)
		if err != nil {
			return nil, err
		}
		fs[i] = field // Field的raw和table信息无关
	}
	raw := WrapTableRaw(tableName, tm.topTableUid, fs, 0)
	if uid, err := tm.vm.Insert(xid, raw, versionManager.MetaDataTbUid); err != nil {
		return nil, err
	} else {
		table := DefaultTableFactory.NewTable(uid, raw, tm)
		for _, field := range fs {
			field.SetTable(table)
		}
		// write lock
		tm.lock.Lock()
		defer tm.lock.Unlock()
		if newFile, err := os.Create("tmp" + bootFileSuf); err != nil {
			panic("Error occurs when creating temporary boot file")
		} else {
			buffer := bytes.NewBuffer([]byte{})
			_ = binary.Write(buffer, binary.BigEndian, uid)
			if _, err := newFile.Write(buffer.Bytes()); err != nil {
				panic("Error occurs when writing temporary boot file")
			} else {
				// delete and rename
				if err := os.Remove(tm.path + bootFileSuf); err != nil {
					panic("Error occurs when writing temporary boot file")
				}
				if err := os.Rename(TMP+bootFileSuf, tm.path+bootFileSuf); err != nil {
					panic("Error occurs when writing temporary boot file")
				}
			}
		}
		tm.tables[table.GetName()] = uid
		tm.tableUid[uid] = table.GetName()
		tm.topTableUid = uid
		return table, nil
	}
}

func (tm *TMImpl) init() {
	tm.loadTables()
}

// loadTables
// 载入所有表
// 仅在启动时操作，不用加锁
func (tm *TMImpl) loadTables() {
	stat, _ := tm.bootFile.Stat()
	if stat.Size() == 0 {
		return
	}
	buf := make([]byte, SzTableUid)
	if _, err := tm.bootFile.ReadAt(buf, 0); err != nil {
		panic("Error occurs when ")
	}
	startUid := int64(binary.BigEndian.Uint64(buf))
	tm.topTableUid = startUid
	uid := startUid
	for uid != 0 {
		record := tm.vm.Read(transactions.SuperXID, uid)
		tableRaw := record.GetData()
		table := DefaultTableFactory.NewTable(uid, tableRaw, tm)
		tm.tables[table.GetName()] = uid
		tm.tableUid[uid] = table.GetName()
		uid = table.GetNextUid()
	}
}

// SearchAll
// 全表扫描
func (tm *TMImpl) searchAll(xid int64, tb Table) ([]Row, error) {
	uid := tb.GetFirstRecordUid()
	var ret []Row
	for uid != 0 {
		record := tm.vm.Read(xid, uid)
		row := DefaultRowFactory.NewRow(tb, record.GetData())
		ret = append(ret, row)
	}
	return ret, nil
}

func (tm *TMImpl) getTbUid(tbName string) (int64, error) {
	tm.lock.RLock()
	defer tm.lock.RUnlock()
	if uid, ext := tm.tables[tbName]; !ext {
		return -1, &ErrorTableNotExist{}
	} else {
		return uid, nil
	}
}

func (tm *TMImpl) getTbName(tbUid int64) (string, error) {
	tm.lock.RLock()
	defer tm.lock.RUnlock()
	if name, ext := tm.tableUid[tbUid]; !ext {
		return "", &ErrorTableNotExist{}
	} else {
		return name, nil
	}
}

func (tm *TMImpl) wrapTableResponseTitle(fName []string) []*ResponseObject {
	rowCnt := 0
	colCnt := 0
	res := make([]*ResponseObject, len(fName))
	for i, fN := range fName {
		res[i] = &ResponseObject{
			payload: fN,
			rowId:   rowCnt,
			colId:   colCnt,
		}
		colCnt += 1
	}
	return res
}

func matchWhereCondition(row Row, table Table, where *Where) bool {
	if where == nil {
		return true
	}
	comparedField := where.compare.fieldName
	var target int
	for i, field := range table.GetFields() {
		if field.GetName() == comparedField {
			target = i
			break
		}
	}
	value1 := row.GetValues()[target]
	value2 := where.compare.value
	compareFunction := DefaultFieldFactory.GetCompareFunction(table.GetFields()[target].GetFType())
	switch where.compare.compareTo {
	case "=":
		{
			return compareFunction(value1, value2) == 0
		}
	case ">":
		{
			return compareFunction(value1, value2) == 1
		}
	case "<":
		{
			return compareFunction(value1, value2) == -1
		}
	case ">=":
		{
			return compareFunction(value1, value2) >= 0
		}
	case "<=":
		{
			return compareFunction(value1, value2) <= 0
		}
	default:
		return false
	}
	return false
}

func NewTableManager(path string, memory int64, mutex *sync.RWMutex) TableManager {
	tm := &TMImpl{
		vm: versionManager.NewVersionManager(path, memory, &sync.RWMutex{}),
		// TODO indexManager
		tables:   map[string]int64{},
		tableUid: map[int64]string{},
		path:     path,
		lock:     mutex,
	}
	if f, err := os.OpenFile(path+bootFileSuf, os.O_RDWR, 0666); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if tryToOpenTmp() != nil {
				tm.bootFile = f
				// rename
				if err := os.Rename(TMP+bootFileSuf, path+bootFileSuf); err != nil {
					panic("Error occurs when initializing table manager")
				}
			} else {
				f, err = os.Create(path + bootFileSuf)
				if err != nil {
					panic("Error occurs when initializing table manager")
				}
			}
		} else {
			panic("Error occurs when initializing table manager")
		}
		tm.bootFile = f
	} else {
		tm.bootFile = f
	}
	// delete TMP
	if err := os.Remove(TMP + bootFileSuf); err != nil {
		panic("Error occurs when initializing table manager")
	}
	tm.init()
	return tm
}

func tryToOpenTmp() *os.File {
	if f, err := os.OpenFile(TMP+bootFileSuf, os.O_RDWR, 0666); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		} else {
			panic("Error occurs when initializing table manager")
		}
	} else {
		return f
	}
}
