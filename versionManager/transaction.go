package versionManager

type IsolationLevel int32

const (
	ReadCommitted  IsolationLevel = 0
	ReadRepeatable IsolationLevel = 1
)

// Transaction 描述事物的抽象
type Transaction struct {
	xid           int64
	level         IsolationLevel
	rv            *ReadView // 读视图
	autoCommitted bool
	vm            VersionManager
}

func NewTransaction(xid int64, level IsolationLevel, autoCommitted bool, vm VersionManager) *Transaction {
	tx := &Transaction{
		xid:           xid,
		level:         level,
		autoCommitted: autoCommitted,
	}
	if level == ReadRepeatable {
		// 生成ReadView
		// TODO
		tx.rv = NewReadView(vm, xid)
	}
	return tx
}
