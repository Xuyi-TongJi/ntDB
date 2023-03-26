package tableManager

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
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
	Commit(xid int64)
	Abort(xid int64)

	Show(xid int64) ([]*ResponseObject, error) // 展示DB中的所有表
	Create(xid int64, create *Create) error    // create table

	Insert(xid int64, insert *Insert) error                 // insert
	Read(xid int64, sel *Select) ([]*ResponseObject, error) // select
	Update(xid int64, update *Update) error                 // update fields
	Delete(xid int64, delete *Delete) error                 // delete

	CreateField(xid int64, fieldName string, fieldType FieldType, indexed bool) (Field, error)
	CreateTable(xid int64, tableName string, fields []*FieldCreate) (Table, error)

	// TODO ADD INDEX

	loadField(tb Table, uid int64) Field
	loadTable(uid int64) Table
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
	tableUid    map[int64]string          // ****uid -> name, 因为表的recordRaw不会增加，因此表的uid永远不会更新
	bootFile    *os.File                  // bootFile里保存一个八字节长度tbUid, 为tb链表的头部，在每次数据库启动时，通过这个文件初始化tableUid
	topTableUid int64
	path        string
	lock        *sync.RWMutex // 保护tables和tableUid(CreateTable和Show)
}

// error

type ErrorTableNotExist struct{}
type ErrorInvalidFieldCount struct{}
type ErrorInvalidFieldName struct{}
type ErrorUnsupportedOperationType struct{}

func (err *ErrorTableNotExist) Error() string {
	return "Table doesn't exist"
}

func (err *ErrorInvalidFieldCount) Error() string {
	return "Field count to be inserted doesn't match with the table"
}

func (err *ErrorInvalidFieldName) Error() string {
	return "The field doesn't exist in this table"
}

func (err *ErrorUnsupportedOperationType) Error() string {
	return "The operation type is not supported"
}

func (tm *TMImpl) Begin() int64 {
	return tm.vm.Begin()
}

func (tm *TMImpl) Commit(xid int64) {
	tm.vm.Commit(xid)
}

func (tm *TMImpl) Abort(xid int64) {
	tm.vm.Abort(xid)
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
	for uid := range tm.tableUid {
		if record := tm.vm.Read(xid, uid); record != nil {
			tabName := tm.tableUid[uid]
			res = append(res, &ResponseObject{tabName, rowId, 0})
			rowId += 1
		}
	}
	return res, nil
}

func (tm *TMImpl) Create(xid int64, create *Create) error {
	_, err := tm.CreateTable(xid, create.TbName, create.Fields)
	return err
}

// Insert
// 需要修改主键
func (tm *TMImpl) Insert(xid int64, insert *Insert) error {
	// check valid
	if uid, err := tm.getTbUid(insert.TbName); err != nil {
		return err
	} else {
		record := tm.vm.Read(xid, uid)
		if record == nil {
			return &ErrorTableNotExist{}
		}
		tb := DefaultTableFactory.NewTable(uid, record.GetData(), tm)
		// 该表对当前事物可见
		// check valid
		insertValueCount := len(insert.Values)
		if insertValueCount != len(tb.GetFields())-1 {
			return &ErrorInvalidFieldCount{}
		}
		// TODO INDEX

		// add primary key
		values := make([]any, len(insert.Values)+1)
		values[0] = tb.GetPrimaryKey()
		for i, value := range insert.Values {
			// to value
			if ret, err := traverseStringToValue(tb.GetFields()[i+1].GetFType(), value); err != nil {
				return err
			} else {
				values[i+1] = ret
			}
		}
		if raw, err := WrapRowRaw(tb, RECORD, int64(0), tb.GetFirstRecordUid(), values); err != nil {
			return err
		} else {
			uid, err := tm.vm.Insert(xid, raw, tb.GetUid())
			if err != nil {
				return err
			}
			newTableRaw := DefaultTableFactory.WrapTableRaw(insert.TbName, tb.GetNextUid(), tb.GetFields(), uid, tb.GetPrimaryKey()+1)
			// 当前事物一定已经获取表锁, 这个Update和上面的Insert一定是原子的
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
func (tm *TMImpl) Read(xid int64, sel *Select) ([]*ResponseObject, error) {
	uid, err := tm.getTbUid(sel.TbName)
	if err != nil {
		return nil, err
	}
	if record := tm.vm.Read(xid, uid); record == nil {
		return nil, &ErrorTableNotExist{}
	} else {
		tb := DefaultTableFactory.NewTable(uid, record.GetData(), tm)
		// check where condition valid
		if sel.Where != nil && sel.Where.Compare != nil {
			if err := tm.checkWhereCondition(tb, sel.Where); err != nil {
				return nil, err
			}
		}
		// read data
		var (
			rows []Row
			err  error
		)
		if sel.ReadForUpdate {
			rows, err = tm.searchAllForUpdate(xid, tb)
		} else {
			rows, err = tm.searchAll(xid, tb)
		}
		if err != nil {
			return nil, err
		}
		targetFieldsId := make([]int, 0)
		for i, field := range tb.GetFields() {
			for _, fN := range sel.FNames {
				if field.GetName() == fN {
					targetFieldsId = append(targetFieldsId, i)
					break
				}
			}
		}
		rowCnt := 1
		// title
		response := tm.wrapTableResponseTitle(sel.FNames)
		// addResponse
		for _, row := range rows {
			if matchWhereCondition(row, tb, sel.Where) {
				colCnt := 0
				for _, target := range targetFieldsId {
					// row.GetValues[target]
					value, _ := fieldValueToBytes(tb.GetFields()[target].GetFType(), row.GetValues()[target])
					response = append(response, &ResponseObject{
						Payload: string(value),
						RowId:   rowCnt,
						ColId:   colCnt,
					})
					colCnt += 1
				}
				rowCnt += 1
			}
		}
		return response, nil
	}
}

// Update
// 上层必须确保在遇到error时回滚
func (tm *TMImpl) Update(xid int64, update *Update) error {
	uid, err := tm.getTbUid(update.TName)
	if err != nil {
		return err
	}
	if record := tm.vm.Read(xid, uid); record == nil {
		return &ErrorTableNotExist{}
	} else {
		tb := DefaultTableFactory.NewTable(uid, record.GetData(), tm)
		// check update valid
		var target = -1
		for i, field := range tb.GetFields() {
			if field.GetName() == update.FName {
				target = i
				break
			}
		}
		if target == -1 {
			return &ErrorInvalidFieldName{}
		}
		var value any
		value, err := traverseStringToValue(tb.GetFields()[target].GetFType(), update.ToUpdate)
		if err != nil {
			return err
		}
		// check where
		if update.Where != nil && update.Where.Compare != nil {
			if err := tm.checkWhereCondition(tb, update.Where); err != nil {
				return err
			}
		}
		rUid := tb.GetFirstRecordUid()
		for rUid != 0 {
			record, err := tm.vm.ReadForUpdate(xid, rUid, tb.GetUid())
			if err != nil {
				tm.Abort(xid)
				return err
			}
			row := DefaultRowFactory.NewRow(rUid, tb, record.GetData())
			if matchWhereCondition(row, tb, update.Where) {
				// update value
				updateValues := row.GetValues()
				updateValues[target] = value
				if raw, err := WrapRowRaw(tb, RECORD, row.GetNextUid(), row.GetNextUid(), updateValues); err != nil {
					tm.Abort(xid)
					return err
				} else {
					newUid, err := tm.vm.Update(xid, uid, tb.GetUid(), raw)
					if err != nil {
						tm.Abort(xid)
						return err
					}
					// uid change
					if newUid != row.GetUid() {
						prevUid := row.GetPrevUid()
						nextUid := row.GetNextUid()
						// modify prev
						if prevUid != 0 {
							// rewrite the previous row
							rec, err := tm.vm.ReadForUpdate(xid, prevUid, tb.GetUid())
							if err != nil {
								tm.Abort(xid)
								return err
							}
							previousRowRaw := rec.GetData()
							row := DefaultRowFactory.NewRow(prevUid, tb, previousRowRaw)
							if raw, err := WrapRowRaw(tb, RECORD, row.GetPrevUid(), newUid, row.GetValues()); err != nil {
								tm.Abort(xid)
								return err
							} else {
								newPrevUid, err := tm.vm.Update(xid, prevUid, tb.GetUid(), raw)
								if newPrevUid != prevUid {
									panic("Fatal Error occurs when updating record raw")
								}
								if err != nil {
									tm.Abort(xid)
									return err
								}
							}
						} else {
							// rewrite the table
							tableRaw := DefaultTableFactory.WrapTableRaw(tb.GetName(), tb.GetNextUid(), tb.GetFields(), newUid, tb.GetPrimaryKey())
							newTableUid, err := tm.vm.Update(xid, tb.GetUid(), tb.GetUid(), tableRaw)
							if newTableUid != tb.GetUid() {
								panic("Fatal Error occurs when updating table metadata raw")
							}
							if err != nil {
								tm.Abort(xid)
								return err
							}
						}
						// modify next
						if nextUid != 0 {
							// rewrite the next row
							rec, err := tm.vm.ReadForUpdate(xid, nextUid, tb.GetUid())
							if err != nil {
								tm.Abort(xid)
								return err
							}
							nextRowRaw := rec.GetData()
							row := DefaultRowFactory.NewRow(nextUid, tb, nextRowRaw)
							if raw, err := WrapRowRaw(tb, RECORD, newUid, row.GetNextUid(), row.GetValues()); err != nil {
								tm.Abort(xid)
								return err
							} else {
								newNextUid, err := tm.vm.Update(xid, nextUid, tb.GetUid(), raw)
								if newNextUid != nextUid {
									panic("Fatal Error occurs when updating record raw")
								}
								if err != nil {
									tm.Abort(xid)
									return err
								}
							}
						}
					}
				}
			}
			rUid = row.GetNextUid()
		}
	}
	return nil
}

// Delete
// 如果没有where子句，则代表删除全表所有的数据
func (tm *TMImpl) Delete(xid int64, delete *Delete) error {
	uid, err := tm.getTbUid(delete.TName)
	if err != nil {
		return err
	}
	if record := tm.vm.Read(xid, uid); record == nil {
		return &ErrorTableNotExist{}
	} else {
		tb := DefaultTableFactory.NewTable(uid, record.GetData(), tm)
		// check where
		if delete.Where != nil && delete.Where.Compare != nil {
			if err := tm.checkWhereCondition(tb, delete.Where); err != nil {
				return err
			}
		} else {
			// delete all
			if err := tm.deleteAll(xid, tb); err != nil {
				tm.Abort(xid)
				return err
			}
		}
		rUid := tb.GetFirstRecordUid()
		for rUid != 0 {
			record, err := tm.vm.ReadForUpdate(xid, rUid, tb.GetUid())
			if err != nil {
				tm.Abort(xid)
				return err
			}
			row := DefaultRowFactory.NewRow(rUid, tb, record.GetData())
			if matchWhereCondition(row, tb, delete.Where) {
				if err := tm.vm.Delete(xid, row.GetUid(), tb.GetUid()); err != nil {
					tm.Abort(xid)
					return err
				}
				if row.GetPrevUid() == 0 {
					// update table
					newTbRaw := DefaultTableFactory.WrapTableRaw(tb.GetName(), tb.GetNextUid(), tb.GetFields(), row.GetNextUid(), tb.GetPrimaryKey())
					if newTbUid, err := tm.vm.Update(xid, tb.GetUid(), tb.GetUid(), newTbRaw); err != nil {
						tm.Abort(xid)
						return err
					} else if newTbUid != tb.GetUid() {
						panic("Fatal Error occurs when updating table metadata raw")
					}
				} else {
					// update previous
					prevRecord, err := tm.vm.ReadForUpdate(xid, row.GetPrevUid(), tb.GetUid())
					if err != nil {
						tm.Abort(xid)
						return err
					}
					prevRaw := prevRecord.GetData()
					prevRow := DefaultRowFactory.NewRow(row.GetPrevUid(), tb, prevRaw)
					raw, err := WrapRowRaw(tb, RECORD, prevRow.GetPrevUid(), row.GetNextUid(), prevRow.GetValues())
					if err != nil {
						tm.Abort(xid)
						return err
					}
					prevUid, err := tm.vm.Update(xid, prevRow.GetUid(), tb.GetUid(), raw)
					if prevUid != row.GetPrevUid() {
						panic("Fatal Error occurs when updating record raw")
					} else if err != nil {
						panic(fmt.Sprintf("Error occurs whne updating record raw, err = %s", err))
					}
				}
				if row.GetNextUid() != 0 {
					nextRecord, err := tm.vm.ReadForUpdate(xid, row.GetNextUid(), tb.GetUid())
					if err != nil {
						tm.Abort(xid)
						return err
					}
					nextRaw := nextRecord.GetData()
					nextRow := DefaultRowFactory.NewRow(row.GetNextUid(), tb, nextRaw)
					raw, err := WrapRowRaw(tb, RECORD, row.GetPrevUid(), row.GetNextUid(), nextRow.GetValues())
					if err != nil {
						tm.Abort(xid)
						return err
					}
					nextUid, err := tm.vm.Update(xid, nextRow.GetUid(), tb.GetUid(), raw)
					if nextUid != row.GetNextUid() {
						panic("Fatal Error occurs when updating record raw")
					} else if err != nil {
						panic(fmt.Sprintf("Error occurs when updating record raw, err = %s", err))
					}
				}
			}
			rUid = row.GetNextUid()
		}
	}
	return nil
}

func (tm *TMImpl) loadField(tb Table, uid int64) Field {
	record := tm.vm.Read(transactions.SuperXID, uid)
	if record == nil {
		panic("Error occurs when loading metadata of field")
	}
	// RECORD
	// [ROLLBACK]8[XID]8[Data length]8[DATA]

	xid := record.GetXid()
	log.Printf("[TABLE MANAGER LINE 464] LOAD FIELD CREATED BY XID = %d\n", xid)

	data := record.GetData()
	// [DATA] -> of field meta data
	return DefaultFieldFactory.NewField(tb, uid, data, tm.im)
}

func (tm *TMImpl) CreateField(xid int64, fieldName string, fieldType FieldType, indexed bool) (Field, error) {
	var indexUid int64 = 0
	if indexed {
		// TODO index
		// indexUid = tm.im.CreateIndex(xid, DefaultFieldFactory.GetCompareFunction(fieldType))
	}
	raw := DefaultFieldFactory.WrapFieldRaw(fieldName, fieldType, indexUid) // format RECORD DATA
	// insert metadata to dm
	if uid, err := tm.vm.Insert(xid, raw, versionManager.MetaDataTbUid); err != nil {
		return nil, err
	} else {
		return DefaultFieldFactory.NewField(nil, uid, raw, tm.im), err // table字段需要后续添加
	}
}

func (tm *TMImpl) loadTable(uid int64) Table {
	record := tm.vm.Read(transactions.SuperXID, uid)
	if record == nil {
		panic("Error occurs when loading metadata of table")
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
		fType, err1 := TransToFieldType(fc.FType)
		if err1 != nil {
			return nil, err1
		}
		indexed, err2 := TransIndexed(fc.Indexed)
		if err2 != nil {
			return nil, err2
		}
		field, err := tm.CreateField(xid, fc.FName, fType, indexed)
		if err != nil {
			return nil, err
		}
		fs[i] = field // Field的raw和table信息无关
	}
	raw := DefaultTableFactory.WrapTableRaw(tableName, tm.topTableUid, fs, 0, 0)
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
	log.Printf("[Table Manager] Initialize table manager")
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
	log.Printf("[Table Manager] Load tables")
}

// SearchAll
// 全表扫描
func (tm *TMImpl) searchAll(xid int64, tb Table) ([]Row, error) {
	uid := tb.GetFirstRecordUid()
	var ret []Row
	for uid != 0 {
		record := tm.vm.Read(xid, uid)
		row := DefaultRowFactory.NewRow(uid, tb, record.GetData())
		ret = append(ret, row)
		uid = row.GetNextUid()
	}
	return ret, nil
}

func (tm *TMImpl) searchAllForUpdate(xid int64, tb Table) ([]Row, error) {
	uid := tb.GetFirstRecordUid()
	var ret []Row
	for uid != 0 {
		if record, err := tm.vm.ReadForUpdate(xid, uid, tb.GetUid()); err != nil {
			return nil, err
		} else {
			row := DefaultRowFactory.NewRow(uid, tb, record.GetData())
			ret = append(ret, row)
			uid = row.GetNextUid()
		}
	}
	return ret, nil
}

func (tm *TMImpl) deleteAll(xid int64, tb Table) error {
	if rows, err := tm.searchAllForUpdate(xid, tb); err != nil {
		return err
	} else {
		for _, row := range rows {
			if err := tm.vm.Delete(xid, row.GetUid(), tb.GetUid()); err != nil {
				return err
			}
		}
		// update table
		newTbRaw := DefaultTableFactory.WrapTableRaw(tb.GetName(), tb.GetNextUid(), tb.GetFields(), int64(0), tb.GetPrimaryKey())
		if newUid, err := tm.vm.Update(xid, tb.GetUid(), tb.GetUid(), newTbRaw); err != nil {
			return err
		} else {
			if newUid != tb.GetUid() {
				panic("Fatal Error occurs when updating table metadata raw")
			}
		}
		return nil
	}
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

func (tm *TMImpl) checkWhereCondition(tb Table, where *Where) error {
	if where == nil || where.Compare == nil {
		return nil
	}
	fieldName := where.Compare.FieldName
	find := false
	for _, field := range tb.GetFields() {
		if field.GetName() == fieldName {
			find = true
			if _, err := traverseStringToValue(field.GetFType(), where.Compare.Value); err != nil {
				return err
			}
			break
		}
	}
	if !find {
		return &ErrorInvalidFieldName{}
	}
	return nil
}

// used for select query
func (tm *TMImpl) wrapTableResponseTitle(fName []string) []*ResponseObject {
	rowCnt := 0
	colCnt := 0
	res := make([]*ResponseObject, len(fName))
	for i, fN := range fName {
		res[i] = &ResponseObject{
			Payload: fN,
			RowId:   rowCnt,
			ColId:   colCnt,
		}
		colCnt += 1
	}
	return res
}

func matchWhereCondition(row Row, table Table, where *Where) bool {
	if where == nil {
		return true
	}
	comparedField := where.Compare.FieldName
	var target int
	for i, field := range table.GetFields() {
		if field.GetName() == comparedField {
			target = i
			break
		}
	}
	value1 := row.GetValues()[target]
	value2 := where.Compare.Value
	compareFunction := DefaultFieldFactory.GetCompareFunction(table.GetFields()[target].GetFType())
	switch where.Compare.CompareTo {
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
}

func NewTableManager(path string, memory int64, mutex *sync.RWMutex, level versionManager.IsolationLevel) TableManager {
	tm := &TMImpl{
		vm: versionManager.NewVersionManager(path, memory, &sync.RWMutex{}, level),
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
	if err := os.Remove(TMP + bootFileSuf); err != nil && !errors.Is(err, os.ErrNotExist) {
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
