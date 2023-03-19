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
	locks         []int64  // 持有的表锁
	action        []Action // 执行的操作, 用于回滚
}

func NewTransaction(xid int64, level IsolationLevel, autoCommitted bool, vm VersionManager) *Transaction {
	tx := &Transaction{
		xid:           xid,
		level:         level,
		autoCommitted: autoCommitted,
		vm:            vm,
	}
	if level == ReadRepeatable {
		// 生成ReadView
		tx.rv = vm.CreateReadView(xid)
	}
	return tx
}

type Action struct {
	aType ActionType
	uid   int64
}

type ActionType int64

const (
	INSERT ActionType = 0
	UPDATE ActionType = 1
	DELETE ActionType = 2
)
