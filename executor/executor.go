package executor

import (
	"log"
	"myDB/storageEngine"
	"myDB/tableManager"
	"myDB/versionManager"
)

type Executor interface {
	Execute(xid int64, args []string) (int64, []*tableManager.ResponseObject, error)
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
func (db *NtDB) Execute(xid int64, args []string) (int64, []*tableManager.ResponseObject, error) {
	cmd, entity, err := db.parser.ParseRequest(args)
	if err != nil {
		return xid, nil, err
	}
	switch cmd {
	case BEGIN:
		{
			// 一个连接不能同时开启两个事物
			if xid != -1 {
				return xid, nil, &ErrorRequestArgNumber{}
			}
			x := db.storageEngine.Begin()
			return x, nil, nil
		}
	case COMMIT:
		{
			if xid == -1 {
				return xid, nil, &ErrorIllegalOperation{}
			}
			db.storageEngine.Commit(xid)
		}
	case ABORT:
		{
			if xid == -1 {
				return xid, nil, &ErrorIllegalOperation{}
			}
			db.storageEngine.Abort(xid)
		}
	case SHOW:
		{
			ret, err := db.storageEngine.Show(xid)
			return xid, ret, err
		}
	case SELECT:
		{
			sel, ok := entity[0].(*tableManager.Select)
			if !ok {
				return xid, nil, &ErrorInvalidEntity{}
			}
			ret, err := db.storageEngine.Select(xid, sel)
			return xid, ret, err
		}
	case UPDATE:
		{
			upd, ok := entity[0].(*tableManager.Update)
			if !ok {
				return xid, nil, &ErrorInvalidEntity{}
			}
			er := db.storageEngine.Update(xid, upd)
			return xid, nil, er
		}
	case INSERT:
		{
			ins, ok := entity[0].(*tableManager.Insert)
			if !ok {
				return xid, nil, &ErrorInvalidEntity{}
			}
			er := db.storageEngine.Insert(xid, ins)
			return xid, nil, er
		}
	case DELETE:
		{
			del, ok := entity[0].(*tableManager.Delete)
			if !ok {
				return xid, nil, &ErrorInvalidEntity{}
			}
			er := db.storageEngine.Delete(xid, del)
			return xid, nil, er
		}
	case CREATE:
		{
			cre, ok := entity[0].(*tableManager.Create)
			if !ok {
				return xid, nil, &ErrorInvalidEntity{}
			}
			er := db.storageEngine.Create(xid, cre)
			return xid, nil, er
		}
	default:
		{
			return xid, nil, &ErrorInvalidEntity{}
		}
	}
	return xid, nil, nil
}

func NewExecutor(path string, memory int64, level versionManager.IsolationLevel) Executor {
	db := &NtDB{
		parser:        NewTrieParser(),
		storageEngine: storageEngine.NewStorageEngine(path, memory, level),
	}
	log.Printf("[Executor] Start executor\n")
	return db
}

type ErrorIllegalOperation struct{}

func (err *ErrorIllegalOperation) Error() string {
	return "Illegal operation"
}
