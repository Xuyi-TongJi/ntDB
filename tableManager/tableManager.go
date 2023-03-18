package tableManager

import (
	"fmt"
	"myDB/indexManager"
	"myDB/transactions"
	"myDB/versionManager"
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

	Insert(xid int64, insert Insert) ([]byte, error) //
	Read(xid int64, sel Select) ([]byte, error)      // select
	Update(xid int64, update Update) ([]byte, error) // update fields
	Delete(xid int64, delete Delete) ([]byte, error)

	LoadField(tb Table, uid int64) Field
	CreateField(tb Table, xid int64, fieldName string, fieldType FieldType, indexed bool) (Field, error)

	LoadTable(uid int64) Table
	CreateTable(nextUid int64, xid int64, tableName string, fields []Field) (Table, error)
}

type TMImpl struct {
	vm versionManager.VersionManager
	im indexManager.IndexManager
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
func (tm *TMImpl) CreateTable(nextUid int64, xid int64, tableName string, fields []Field) (Table, error) {
	raw := WrapTableRaw(tableName, nextUid, fields)
	if uid, err := tm.vm.Insert(xid, raw, versionManager.MetaDataTbUid); err != nil {
		return nil, err
	} else {
		return DefaultTableFactory.NewTable(uid, raw, tm), nil
	}
}
