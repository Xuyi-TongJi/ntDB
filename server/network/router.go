package network

import (
	"fmt"
	"log"
	"myDB/executor"
	"myDB/server/iface"
	"myDB/tableManager"
	"myDB/versionManager"
	"strconv"
	"strings"
	"time"
)

const (
	DbRouterMsgId        = 0x3f
	TRANS         string = "trans"
	AUTO          string = "auto"
)

// 路由模块，实现路由接口
// 不同消息对应不同的处理方式

// BaseRouter 基类 BaseRouter的方法都为空，是因为可以让Router的具体实现更灵活的实现这三个方法
type BaseRouter struct {
	name string
}

func (b *BaseRouter) PreHandle(request iface.IRequest) {}

func (b *BaseRouter) DoHandle(request iface.IRequest) {}

func (b *BaseRouter) PostHandle(request iface.IRequest) {}

// DbRouter 自定义Router
type DbRouter struct {
	BaseRouter
	db executor.Executor
}

func (dbRouter *DbRouter) PreHandle(request iface.IRequest) {

}

func (dbRouter *DbRouter) DoHandle(request iface.IRequest) {
	args := request.GetArgs()
	var (
		err      error
		response []*tableManager.ResponseObject
	)
	log.Printf("[Database] Prepare to handle the requeset of connection %d", request.GetConnection().GetConnId())
	if len(args) == 0 {
		err = &ErrorIllegalOperation{}
	} else {
		if dbRouter.isBeginCommand(args) {
			err = dbRouter.doBegin(false, request)
		} else if dbRouter.isAbortCommand(args) {
			err = dbRouter.doAbort(request)
		} else if dbRouter.isCommitCommand(args) {
			err = dbRouter.doCommit(request)
		} else if dbRouter.isQuitCommand(args) {
			dbRouter.doQuit(request)
		} else {
			if xid := request.GetConnection().GetConnectionProperty(TRANS); xid == nil || (xid).(int64) == -1 {
				err = dbRouter.doBegin(true, request)
			}
			xid := request.GetConnection().GetConnectionProperty(TRANS).(int64)
			_, response, err = dbRouter.db.Execute(xid, args)
		}
		log.Printf("[Database] Connection %d finish a command %s, xid = %d\n",
			request.GetConnection().GetConnId(), request.GetArgs()[0], request.GetConnection().GetConnectionProperty(TRANS))
	}
	if err != nil {
		// handle error
		dbRouter.handleError(err, request)
	} else if response != nil && len(response) > 0 {
		// json
		last := response[len(response)-1]
		row, col := last.ColId, last.RowId
		resMsg := make([]string, 0)
		resMsg = append(resMsg, strconv.FormatInt(int64(row), 10))
		resMsg = append(resMsg, strconv.FormatInt(int64(col), 10))
		for _, res := range response {
			resMsg = append(resMsg, res.Payload)
		}
		request.GetConnection().SendMessage([]byte(packBulkArray(resMsg)))
	} else {
		// query ok
		request.GetConnection().SendMessage([]byte(packString(OK)))
	}
}

func (dbRouter *DbRouter) PostHandle(request iface.IRequest) {
	// 如果设置了自动提交，则执行结束后自动提交事物
	if auto := request.GetConnection().GetConnectionProperty(AUTO).(bool); auto {
		// 自动提交
		if err := dbRouter.doCommit(request); err != nil {
			dbRouter.handleError(err, request)
		}
	}
}

func (dbRouter *DbRouter) beginTransactionAuto(request iface.IRequest) error {
	return dbRouter.doBegin(true, request)
}

func (dbRouter *DbRouter) isQuitCommand(args []string) bool {
	return len(args) > 0 && strings.ToUpper(args[0]) == "QUIT"
}

func (dbRouter *DbRouter) isBeginCommand(args []string) bool {
	return len(args) > 0 && strings.ToUpper(args[0]) == "BEGIN"
}

func (dbRouter *DbRouter) isAbortCommand(args []string) bool {
	return len(args) > 0 && strings.ToUpper(args[0]) == "ABORT"
}

func (dbRouter *DbRouter) isCommitCommand(args []string) bool {
	return len(args) > 0 && strings.ToUpper(args[0]) == "COMMIT"
}

func (dbRouter *DbRouter) doBegin(autoCommitted bool, request iface.IRequest) error {
	xid := request.GetConnection().GetConnectionProperty(TRANS)
	// 一个连接只能同时开启一个事物
	if xid != nil && xid.(int64) != -1 {
		return &ErrorIllegalOperation{}
	} else {
		args := []string{"BEGIN"}
		x, _, err := dbRouter.db.Execute(-1, args)
		if err != nil {
			return err
		}
		request.GetConnection().SetConnectionProperty(TRANS, x)
		request.GetConnection().SetConnectionProperty(AUTO, autoCommitted)
	}
	return nil
}

// 遇到任何错误一定要直接执行此方法
// DB层只保证写操作如果发生错误一定会回滚，网络层保证遇到一切错误均回滚，多次回滚一个事物不会panic
func (dbRouter *DbRouter) doAbort(request iface.IRequest) error {
	defer func() {
		// remove
		request.GetConnection().SetConnectionProperty(TRANS, int64(-1))
		request.GetConnection().SetConnectionProperty(TRANS, false)
	}()
	xid := request.GetConnection().GetConnectionProperty(TRANS)
	if xid == nil || xid.(int64) == -1 {
		return &ErrorIllegalOperation{}
	} else {
		args := []string{"ABORT"}
		_, _, err := dbRouter.db.Execute(xid.(int64), args)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dbRouter *DbRouter) doCommit(request iface.IRequest) error {
	defer func() {
		// remove
		request.GetConnection().SetConnectionProperty(TRANS, int64(-1))
		request.GetConnection().SetConnectionProperty(AUTO, false)
	}()
	xid := request.GetConnection().GetConnectionProperty(TRANS)
	if xid == nil || xid.(int64) == -1 {
		return &ErrorIllegalOperation{}
	} else {
		args := []string{"COMMIT"}
		_, _, err := dbRouter.db.Execute(xid.(int64), args)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dbRouter *DbRouter) doQuit(request iface.IRequest) {
	// 是否未回滚
	dbRouter.abortBeforeErrorOrQuit(request)
	request.GetConnection().SendMessage([]byte(packString(QUIT)))
	time.Sleep(50 * time.Millisecond)
	request.GetConnection().Stop()
}

func (dbRouter *DbRouter) handleError(err error, request iface.IRequest) {
	// 是否未回滚
	dbRouter.abortBeforeErrorOrQuit(request)
	request.GetConnection().SendMessage([]byte(packErrorMessage(fmt.Sprintf("%s, current transaction has been aborted\n", err.Error()))))
}

func (dbRouter *DbRouter) abortBeforeErrorOrQuit(request iface.IRequest) {
	// 尚有事物未提交
	if xid := request.GetConnection().GetConnectionProperty(TRANS); xid != nil && xid.(int64) != -1 {
		_ = dbRouter.doAbort(request)
	}
}

func NewDbRouter(path string, memory int64, level versionManager.IsolationLevel) iface.IRouter {
	return &DbRouter{
		BaseRouter: BaseRouter{"DbRouter"},
		db:         executor.NewExecutor(path, memory, level), // 启动db
	}
}

// error

type ErrorIllegalOperation struct{}

func (err *ErrorIllegalOperation) Error() string {
	return "Illegal Operation"
}

// pack response

const (
	ErrorHead      string = "-ERROR: "
	StringHead     string = "+"
	IntHead        string = ":"
	BulkStringHead string = "$"
	BulkArrayHead  string = "*"
	CRLF           string = "\r\n"
	WELCOME        string = "+Welcome!\r\n"
	OK             string = "Query OK!"
	QUIT           string = "Bye Bye~"
)

func packErrorMessage(msg string) string {
	var str []string = []string{ErrorHead, msg, CRLF}
	return strings.Join(str, "")
}

func packString(msg string) string {
	var str []string = []string{StringHead, msg, CRLF}
	return strings.Join(str, "")
}

func packInt(num int) string {
	var str []string = []string{IntHead, strconv.Itoa(num), CRLF}
	return strings.Join(str, "")
}

func packBulkString(msg string) string {
	len := strconv.Itoa(len(msg))
	var builder strings.Builder
	builder.WriteString(BulkStringHead)
	builder.WriteString(len)
	builder.WriteString(CRLF)
	builder.WriteString(msg)
	builder.WriteString(CRLF)
	return builder.String()
}

func packBulkArray(msg []string) string {
	n := len(msg)
	var builder strings.Builder
	// head
	builder.WriteString(BulkArrayHead)
	builder.WriteString(strconv.Itoa(n))
	builder.WriteString(CRLF)
	for i := 0; i < n; i += 1 {
		builder.WriteString(packBulkString(msg[i]))
	}
	return builder.String()
}
