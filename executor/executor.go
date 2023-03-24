package executor

import (
	"myDB/storageEngine"
	"myDB/tableManager"
)

type Executor interface {
	Execute(xid int64, args []string) ([]*tableManager.ResponseObject, error)
	PackageResponse([]*tableManager.ResponseObject)
}

// CommandType 用于路由
type CommandType int64

const (
	BEGIN   CommandType = 0x01
	COMMIT  CommandType = 0x02
	ABORT   CommandType = 0x03
	SHOW    CommandType = 0x04
	CREATE  CommandType = 0x05
	INSERT  CommandType = 0x06
	SELECT  CommandType = 0x07
	UPDATE  CommandType = 0x08
	DELETE  CommandType = 0x09
	INVALID CommandType = 0xff
)

type NtDB struct {
	parser        Parser
	storageEngine storageEngine.StorageEngine
}

// Execute 向存储引擎请求 xid事物执行指令
// response 可能返回nil
func (db *NtDB) Execute(xid int64, args []string) ([]*tableManager.ResponseObject, error) {
	cmd, entity, err := db.parser.ParseRequest(args)
	if err != nil {
		return nil, err
	}
	switch cmd {
	case BEGIN:
		{
			db.storageEngine.Begin()
		}
	case COMMIT:
		{
			db.storageEngine.Commit(xid)
		}
	case ABORT:
		{
			db.storageEngine.Abort(xid)
		}
	case SHOW:
		{
			return db.storageEngine.Show(xid)
		}
	case SELECT:
		{
			sel, ok := entity[0].(*tableManager.Select)
			if !ok {
				return nil, &ErrorInvalidEntity{}
			}
			return db.storageEngine.Select(xid, sel)
		}
	case UPDATE:
		{
			upd, ok := entity[0].(*tableManager.Update)
			if !ok {
				err = &ErrorInvalidEntity{}
				return nil, &ErrorInvalidEntity{}
			}
			er := db.storageEngine.Update(xid, upd)
			return nil, er
		}
	case INSERT:
		{
			ins, ok := entity[0].(*tableManager.Insert)
			if !ok {
				return nil, &ErrorInvalidEntity{}
			}
			er := db.storageEngine.Insert(xid, ins)
			return nil, er
		}
	case DELETE:
		{
			del, ok := entity[0].(*tableManager.Delete)
			if !ok {
				return nil, &ErrorInvalidEntity{}
			}
			er := db.storageEngine.Delete(xid, del)
			return nil, er
		}
	case CREATE:
		{
			cre, ok := entity[0].(*tableManager.Create)
			if !ok {
				return nil, &ErrorInvalidEntity{}
			}
			er := db.storageEngine.Create(xid, cre)
			return nil, er
		}
	default:
		{
			return nil, &ErrorInvalidEntity{}
		}
	}
	return nil, nil
}

func (db *NtDB) PackageResponse(responses []*tableManager.ResponseObject) {
	//TODO implement me
	panic("implement me")
}

func NewExecutor(path string, memory int64) Executor {
	return &NtDB{
		parser:        NewTrieParser(),
		storageEngine: storageEngine.NewStorageEngine(path, memory),
	}
}
