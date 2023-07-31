package storageEngine

import (
	"log"
	"myDB/tableManager"
	"myDB/versionManager"
	"strconv"
	"sync"
)

type StorageEngine interface {
	Begin() int64 // 开启一个事物，返回xid
	Commit(xid int64)
	Abort(xid int64)

	Show(xid int64) ([]*tableManager.ResponseObject, error) // 展示DB中的所有表
	Create(xid int64, create *tableManager.Create) error    // create table

	Insert(xid int64, insert *tableManager.Insert) ([]*tableManager.ResponseObject, error) // insert
	Select(xid int64, sel *tableManager.Select) ([]*tableManager.ResponseObject, error)    // select
	Update(xid int64, update *tableManager.Update) ([]*tableManager.ResponseObject, error) // update fields
	Delete(xid int64, delete *tableManager.Delete) ([]*tableManager.ResponseObject, error)
}

type NtStorageEngine struct {
	tm tableManager.TableManager
}

type ErrorInvalidParameter struct{}
type ErrorModifyPrimaryKey struct{}
type ErrorRepetitiveField struct{}

var zeroResponseObjectSlice = []*tableManager.ResponseObject{
	{
		Payload: strconv.Itoa(0), ColId: 0, RowId: 0,
	},
}

func (err *ErrorInvalidParameter) Error() string {
	return "query with invalid parameters"
}

func (err *ErrorModifyPrimaryKey) Error() string {
	return "updating primary key value is banned"
}

func (err *ErrorRepetitiveField) Error() string {
	return "Table must not have repetitive field names"
}

func (se *NtStorageEngine) Begin() int64 {
	return se.tm.Begin()
}

func (se *NtStorageEngine) Commit(xid int64) {
	if xid == -1 {
		return
	}
	se.tm.Commit(xid)
}

func (se *NtStorageEngine) Abort(xid int64) {
	if xid == -1 {
		return
	}
	se.tm.Abort(xid)
}

func (se *NtStorageEngine) Show(xid int64) ([]*tableManager.ResponseObject, error) {
	return se.tm.Show(xid)
}

func (se *NtStorageEngine) Create(xid int64, create *tableManager.Create) error {
	if xid == -1 || create == nil || create.TbName == "" {
		return &ErrorModifyPrimaryKey{}
	}
	// 添加主键
	fc := []*tableManager.FieldCreate{{FName: tableManager.PrimaryKeyCol, FType: "int64", Indexed: "indexed"}}
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

func (se *NtStorageEngine) Insert(xid int64, insert *tableManager.Insert) ([]*tableManager.ResponseObject, error) {
	if insert == nil || insert.TbName == "" || insert.Values == nil {
		return zeroResponseObjectSlice, &ErrorInvalidParameter{}
	}
	change, err := se.tm.Insert(xid, insert)
	return []*tableManager.ResponseObject{wrapChangeToResponseObject(change)}, err
}

func (se *NtStorageEngine) Select(xid int64, sel *tableManager.Select) ([]*tableManager.ResponseObject, error) {
	if sel == nil || sel.TbName == "" || sel.FNames == nil {
		return nil, &ErrorInvalidParameter{}
	}
	return se.tm.Read(xid, sel)
}

func (se *NtStorageEngine) Update(xid int64, update *tableManager.Update) ([]*tableManager.ResponseObject, error) {
	// 不可以修改主键字段的值
	if update == nil || update.ToUpdate == "" || update.FName == "" || update.TName == "" {
		return zeroResponseObjectSlice, &ErrorInvalidParameter{}
	}
	if update.FName == tableManager.PrimaryKeyCol {
		return zeroResponseObjectSlice, &ErrorInvalidParameter{}
	}
	change, err := se.tm.Update(xid, update)
	return []*tableManager.ResponseObject{wrapChangeToResponseObject(change)}, err
}

func (se *NtStorageEngine) Delete(xid int64, delete *tableManager.Delete) ([]*tableManager.ResponseObject, error) {
	if delete == nil || delete.TName == "" {
		return zeroResponseObjectSlice, &ErrorInvalidParameter{}
	}
	change, err := se.tm.Delete(xid, delete)
	return []*tableManager.ResponseObject{wrapChangeToResponseObject(change)}, err
}

// 将tableManager返回的影响行数转变为ResponseObject
func wrapChangeToResponseObject(change int64) *tableManager.ResponseObject {
	return &tableManager.ResponseObject{
		Payload: strconv.Itoa(int(change)),
		ColId:   0,
		RowId:   0,
	}
}

func NewStorageEngine(path string, memory int64, level versionManager.IsolationLevel) StorageEngine {
	se := &NtStorageEngine{
		tm: tableManager.NewTableManager(path, memory, &sync.RWMutex{}, level),
	}
	log.Printf("[Storage Engine] Start storage engine\n")
	return se
}
