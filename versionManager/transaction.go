package versionManager

type IsolationLevel int32

const (
	ReadCommitted  IsolationLevel = 0 // 读已提交，每次快照读时创建读视图
	ReadRepeatable IsolationLevel = 1 // 可重复读, 仅在事物开始时创建读视图
)

// Transaction 描述事物的抽象
type Transaction struct {
	xid           int64
	level         IsolationLevel
	rv            *ReadView // 读视图
	autoCommitted bool
	vm            VersionManager
	channel       chan struct{}
}

func NewTransaction(xid int64, level IsolationLevel, autoCommitted bool, vm VersionManager) *Transaction {
	tx := &Transaction{
		xid:           xid,
		level:         level,
		autoCommitted: autoCommitted,
		vm:            vm,
		channel:       make(chan struct{}),
	}
	if level == ReadRepeatable {
		// 生成ReadView
		tx.rv = vm.CreateReadView(xid)
	}
	return tx
}
