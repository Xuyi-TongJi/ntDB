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
	Create(create Create) ([]byte, error)

	Insert(xid int64, insert Insert) ([]byte, error)
	Read(xid int64, sel Select) ([]byte, error)
	Update(xid int64, update Update) ([]byte, error)
	Delete(xid int64, delete Delete) ([]byte, error)

	LoadField(tb Table, uid int64) Field
	CreateField(tb Table, xid int64, fieldName string, fieldType FieldType, indexed bool) Field
}

type TMImpl struct {
	vm versionManager.VersionManager
	im indexManager.IndexManager
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

func (tm *TMImpl) CreateField(tb Table, xid int64, fieldName string, fieldType FieldType, indexed bool) Field {
	var indexUid int64 = 0
	if indexed {
		indexUid = tm.im.CreateIndex(DefaultFieldFactory.GetCompareFunction(fieldType))
	}
	raw := WrapFieldRaw(fieldName, fieldType, indexUid) // format RECORD DATA
	// insert metadata to dm
	uid := tm.vm.Insert(xid, raw)
	return DefaultFieldFactory.NewField(tb, uid, raw, tm.im)
}
