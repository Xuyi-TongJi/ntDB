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
	locks         []int64   // 持有的表锁
	action        []*Action // 执行的操作, 用于回滚
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
	aType  ActionType
	oldUid int64
	oldRaw []byte
	newUid int64
	newRaw []byte
}

type ActionType int64

const (
	INSERT ActionType = 0
	UPDATE ActionType = 1
	DELETE ActionType = 2
)

func (t *Transaction) AddUpdate(oldUid, newUid int64, oldRaw, newRaw []byte) {
	action := &Action{
		aType:  UPDATE,
		oldUid: oldUid,
		oldRaw: oldRaw,
		newUid: newUid,
		newRaw: newRaw,
	}
	t.action = append(t.action, action)
}

func (t *Transaction) AddDelete(uid int64) {
	action := &Action{
		aType:  DELETE,
		newUid: uid,
	}
	t.action = append(t.action, action)
}

func (t *Transaction) AddInsert(uid int64) {
	action := &Action{
		aType:  INSERT,
		newUid: uid,
	}
	t.action = append(t.action, action)
}
