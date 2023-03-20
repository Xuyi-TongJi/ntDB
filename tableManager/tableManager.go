package tableManager

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
	Begin(begin Begin) BeginRes
	Commit(xid int64) ([]byte, error)
	Abort(xid int64) ([]byte, error)

	Show(xid int64) ([]byte, error)
	Create(create Create) ([]byte, error) // create table

	Insert(xid int64, insert Insert) ([]byte, error)
	Read(xid int64, sel Select) ([]byte, error)      // select
	Update(xid int64, update Update) ([]byte, error) // update fields
	Delete(xid int64, delete Delete) ([]byte, error)

	LoadField(tb Table, uid int64) Field
	CreateField(tb Table, xid int64, fieldName string, fieldType FieldType, indexed bool) (Field, error)

	LoadTable(uid int64) Table
	CreateTable(xid int64, tableName string, fields []Field) (Table, error)
}

const (
	TMP         string = "tmp"
	SzMask      int64  = 4 // 字段类型Mask
	bootFileSuf string = ".boot"
)

type TMImpl struct {
	vm          versionManager.VersionManager
	im          indexManager.IndexManager // 保留字段, 实现索引后使用
	tables      map[string]Table          // name -> table
	tableUid    map[int64]string          // uid -> name
	bootFile    *os.File                  // bootFile里保存一个八字节长度tbUid, 为tb链表的头部，在每次数据库启动时，通过这个文件初始化tableUid
	topTableUid int64
	path        string
}

func (tm *TMImpl) Begin(begin Begin) BeginRes {
	//TODO implement me
	panic("implement me")
}

func (tm *TMImpl) Commit(xid int64) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (tm *TMImpl) Abort(xid int64) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (tm *TMImpl) Show(xid int64) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (tm *TMImpl) Create(create Create) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (tm *TMImpl) Insert(xid int64, insert Insert) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (tm *TMImpl) Read(xid int64, sel Select) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (tm *TMImpl) Update(xid int64, update Update) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (tm *TMImpl) Delete(xid int64, delete Delete) ([]byte, error) {
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

func (tm *TMImpl) CreateField(tb Table, xid int64, fieldName string, fieldType FieldType, indexed bool) (Field, error) {
	var indexUid int64 = 0
	if indexed {
		indexUid = tm.im.CreateIndex(DefaultFieldFactory.GetCompareFunction(fieldType))
	}
	raw := WrapFieldRaw(fieldName, fieldType, indexUid) // format RECORD DATA
	// insert metadata to dm
	if uid, err := tm.vm.Insert(xid, raw, tb.GetUid()); err != nil {
		return nil, err
	} else {
		return DefaultFieldFactory.NewField(tb, uid, raw, tm.im), err
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
func (tm *TMImpl) CreateTable(xid int64, tableName string, fields []Field) (Table, error) {
	raw := WrapTableRaw(tableName, tm.topTableUid, fields)
	if uid, err := tm.vm.Insert(xid, raw, versionManager.MetaDataTbUid); err != nil {
		return nil, err
	} else {
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
		tm.topTableUid = uid
		table := DefaultTableFactory.NewTable(uid, raw, tm)
		tm.tables[table.GetName()] = table
		tm.tableUid[uid] = table.GetName()
		return table, nil
	}
}

func (tm *TMImpl) init() {
	tm.loadTables()
}

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
		tm.tables[table.GetName()] = table
		tm.tableUid[uid] = table.GetName()
		uid = table.GetNextUid()
	}
}

func NewTableManager(path string, memory int64) TableManager {
	tm := &TMImpl{
		vm: versionManager.NewVersionManager(path, memory, &sync.RWMutex{}),
		// TODO indexManager
		tables: map[string]Table{},
		path:   path,
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
