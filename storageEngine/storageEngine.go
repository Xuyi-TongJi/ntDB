package storageEngine

import (
	"log"
	"myDB/tableManager"
	"myDB/versionManager"
	"sync"
)

type StorageEngine interface {
	Begin() int64 // 开启一个事物，返回xid
	Commit(xid int64)
	Abort(xid int64)

	Show(xid int64) ([]*tableManager.ResponseObject, error) // 展示DB中的所有表
	Create(xid int64, create *tableManager.Create) error    // create table

	Insert(xid int64, insert *tableManager.Insert) error                                // insert
	Select(xid int64, sel *tableManager.Select) ([]*tableManager.ResponseObject, error) // select
	Update(xid int64, update *tableManager.Update) error                                // update fields
	Delete(xid int64, delete *tableManager.Delete) error
}

type NtStorageEngine struct {
	tm tableManager.TableManager
}

func (se *NtStorageEngine) Begin() int64 {
	return se.tm.Begin()
}

func (se *NtStorageEngine) Commit(xid int64) {
	se.tm.Commit(xid)
}

func (se *NtStorageEngine) Abort(xid int64) {
	se.tm.Abort(xid)
}

func (se *NtStorageEngine) Show(xid int64) ([]*tableManager.ResponseObject, error) {
	return se.tm.Show(xid)
}

type ErrorRepetitiveField struct{}

func (err *ErrorRepetitiveField) Error() string {
	return "Table must not have repetitive field names"
}

func (se *NtStorageEngine) Create(xid int64, create *tableManager.Create) error {
	// 添加主键
	fc := []*tableManager.FieldCreate{{FName: "ID", FType: "int64", Indexed: "indexed"}}
	// 检查是否有重名字段
	fc = append(fc, create.Fields...)
	for i, f1 := range create.Fields {
		for j, f2 := range create.Fields {
			if i != j && f1.FName == f2.FName {
				return &ErrorRepetitiveField{}
			}
		}
	}
	create.Fields = fc
	return se.tm.Create(xid, create)
}

func (se *NtStorageEngine) Insert(xid int64, insert *tableManager.Insert) error {
	return se.tm.Insert(xid, insert)
}

func (se *NtStorageEngine) Select(xid int64, sel *tableManager.Select) ([]*tableManager.ResponseObject, error) {
	return se.tm.Read(xid, sel)
}

func (se *NtStorageEngine) Update(xid int64, update *tableManager.Update) error {
	return se.tm.Update(xid, update)
}

func (se *NtStorageEngine) Delete(xid int64, delete *tableManager.Delete) error {
	return se.tm.Delete(xid, delete)
}

func NewStorageEngine(path string, memory int64, level versionManager.IsolationLevel) StorageEngine {
	se := &NtStorageEngine{
		tm: tableManager.NewTableManager(path, memory, &sync.RWMutex{}, level),
	}
	log.Printf("[Storage Engine] Start storage engine\n")
	return se
}
