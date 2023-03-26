package versionManager

import (
	"errors"
	"fmt"
	"log"
	"myDB/dataManager"
	"myDB/transactions"
	"sync"
)

// VersionManager
// 控制事物的并发执行，快照读为无锁实现

// V1.0 超级事物只会进行快照读 -> 在启动时读取表的元数据
// 启动时不可能有活跃事物(Crash Recovery)，不需要进行init

type VersionManager interface {
	Read(xid, uid int64) Record
	ReadForUpdate(xid, uid, tbUid int64) (Record, error)         // ReadForUpdate 当前读
	Update(xid, uid, tbUid int64, newData []byte) (int64, error) // Update 更新 返回更新后的uid
	Insert(xid int64, data []byte, tbUid int64) (int64, error)   // Insert 返回插入位置(uid)
	Delete(xid, uid, tbUid int64) error
	CreateReadView(xid int64) *ReadView // 创建读视图

	Begin() int64
	Commit(xid int64)
	Abort(xid int64)
}

type VmImpl struct {
	dm             dataManager.DataManager
	tm             transactions.TransactionManager
	undo           Log
	activeTrans    map[int64]*Transaction
	nextXid        int64 // 下一个事物的xid
	minActiveXid   int64 // 当前活跃的事物最小xid
	lt             LockTable
	lock           *sync.RWMutex
	isolationLevel IsolationLevel
}

const (
	MetaDataTbUid   int64 = -1
	MaxTryLockCount int   = 3
)

// Read
// 快照读 MVCC
// 超级事物只会进行快照读
// 可能返回nil
func (v *VmImpl) Read(xid, uid int64) Record {
	transaction := v.getTransaction(xid)
	if transaction == nil {
		panic("Error occurs when getting transaction struct, it is not an active transaction")
	}
	if transaction.level == ReadCommitted {
		transaction.rv = v.CreateReadView(xid)
	}
	di := v.dm.ReadSnapShot(uid) // DataItem
	if di == nil {
		return nil
	}
	snapShot := DefaultRecordFactory.NewSnapShot(di.GetData(), v.undo)
	// 超级事物读取的
	if xid == transactions.SuperXID {
		return snapShot
	}
	for snapShot != nil && !v.checkMvccValid(snapShot, xid) {
		snapShot = snapShot.GetPrevious()
	}
	if !snapShot.IsValid() {
		return nil
	}
	return snapShot
}

// ReadForUpdate
// 当前读，读取最新的数据，如果在dataItem层面已经失效，那么返回nil
// 可能返回nil
func (v *VmImpl) ReadForUpdate(xid, uid, tbUid int64) (Record, error) {
	transaction := v.getTransaction(xid) // check valid
	if transaction == nil {
		panic("Error occurs when getting transaction struct, it is not an active transaction")
	}
	if err := v.tryToLockTable(xid, tbUid); err != nil {
		return nil, err
	}
	di := v.dm.Read(uid)
	if di == nil {
		return nil, nil
	}
	return DefaultRecordFactory.NewRecord(di.GetData(), di, v, uid, v.undo), nil
}

// Update
// 当前读出uid中的数据，如果已经invalid，则panic
// 返回这条record新的uid（见DM的Update方法执行逻辑）
func (v *VmImpl) Update(xid, uid, tbUid int64, newData []byte) (int64, error) {
	tran := v.getTransaction(xid) // check valid
	if tran == nil {
		panic("Error occurs when getting transaction struct, it is not an active transaction")
	}
	// readForUpdate 获取表锁
	if record, err := v.ReadForUpdate(xid, uid, tbUid); err != nil {
		return -1, err
	} else {
		if record == nil {
			panic("Error occurs when updating records, it is an invalid record")
		}
		// undoLog
		rollback := v.undo.Log(record.GetRaw())
		newRecordRaw := WrapRecordRaw(true, newData, xid, rollback)
		newUid := v.dm.Update(xid, uid, newRecordRaw)
		tran.AddUpdate(uid, newUid, record.GetRaw(), newRecordRaw)
		return newUid, err
	}
}

// Insert
// 向DataManager插入数据
// 可以插入元数据（插入表Table信息），也可以插入（索引，数据，字段信息），后者要先获取表锁
// 当插入元数据时，tbUid == -1
// 返回DataItem的uid
func (v *VmImpl) Insert(xid int64, data []byte, tbUid int64) (int64, error) {
	tran := v.getTransaction(xid) // check valid
	if tran == nil {
		panic("Error occurs when getting transaction struct, it is not an active transaction")
	}
	log.Printf("[VERSION MANAGER LINE 144] Transaction %d INSERT..\n", xid)
	// metaData, 不需要获得锁，直接插入, 但是在插入结束后，xid会直接获得这个uid的锁
	if tbUid == MetaDataTbUid {
		return v.dm.Insert(xid, data), nil
	}
	if err := v.tryToLockTable(xid, tbUid); err != nil {
		return -1, err
	}
	uid := v.dm.Insert(xid, data)
	tran.AddInsert(uid)
	// 插入的是表元数据，xid获得uid的锁, must success
	if tbUid == MetaDataTbUid {
		_ = v.tryToLockTable(xid, uid)
	}
	log.Printf("[VERSION MANAGER LINE 144] Transaction %d INSERT FINISHED\n", xid)
	return uid, nil
}

// Delete
// 删除一条记录
// 2步： step1 -> 当前读出record, 将record调用dm.update为invalid step2 -> 调用dm层的Delete方法将uid所在dataItem置为invalid
func (v *VmImpl) Delete(xid, uid, tbUid int64) error {
	tran := v.getTransaction(xid) // check valid
	if tran == nil {
		panic("Error occurs when getting transaction struct, it is not an active transaction")
	}
	// add lock
	record, err := v.ReadForUpdate(xid, uid, tbUid)
	if err != nil {
		return err
	}
	// undoLog
	rollback := v.undo.Log(record.GetRaw())
	newRecordRaw := WrapRecordRaw(false, record.GetData(), xid, rollback)
	newUid := v.dm.Update(xid, uid, newRecordRaw) // newUid == uid
	if newUid != uid {
		panic("Fatal error when updating records")
	}
	tran.AddUpdate(uid, newUid, record.GetRaw(), newRecordRaw)
	v.dm.Delete(xid, uid)
	tran.AddDelete(uid)
	return nil
}

func (v *VmImpl) CreateReadView(xid int64) *ReadView {
	v.lock.RLock()
	defer v.lock.RUnlock()
	var active []int64
	for xid := range v.activeTrans {
		active = append(active, xid)
	}
	return &ReadView{
		creatorId: xid,
		active:    active,
		maxXid:    v.nextXid,
		minXid:    v.minActiveXid,
	}
}

// Begin
// 开启一个新的事物
func (v *VmImpl) Begin() int64 {
	v.lock.Lock()
	defer v.lock.Unlock()
	xid := v.tm.Begin()
	trans := NewTransaction(xid, v.isolationLevel, v)
	if xid+1 > v.nextXid {
		v.nextXid = xid + 1
	}
	v.activeTrans[xid] = trans
	return xid
}

// Commit
// 提交事物
// 释放该事物持有的所有锁
// 更新vm状态
func (v *VmImpl) Commit(xid int64) {
	tran := v.getTransaction(xid)
	if tran == nil {
		return
	}
	v.lock.Lock()
	defer v.lock.Unlock()
	v.endTransaction(xid, tran)
	// tm
	v.tm.Commit(xid)
}

// Abort
// 回滚事物
// 回滚时，因为该事物做出过写操作，因此一定持有相应的表锁，可以直接进行写操作
// 释放该事物持有的所有锁
// 更新vm状态
func (v *VmImpl) Abort(xid int64) {
	tran := v.getTransaction(xid)
	if tran == nil {
		return
	}
	v.lock.Lock()
	defer v.lock.Unlock()
	n := len(tran.action)
	for i := n - 1; i >= 0; i-- {
		switch tran.action[i].aType {
		case UPDATE:
			{
				if tran.action[i].newUid == tran.action[i].oldUid {
					// newUid == oldUid 原地修改
					_ = v.dm.Update(xid, tran.action[i].oldUid, tran.action[i].oldRaw)
				} else {
					// newUid != oldUid 让新的失效，旧的重新valid
					v.dm.Delete(xid, tran.action[i].newUid)
					v.dm.Recover(xid, tran.action[i].oldUid)
				}
			}
		case DELETE:
			{
				v.dm.Recover(xid, tran.action[i].oldUid)
			}
		case INSERT:
			{
				v.dm.Delete(xid, tran.action[i].newUid)
			}
		default:
			panic("Error occurs when roll back, invalid action type")
		}
	}
	v.endTransaction(xid, tran)
	// tm
	v.tm.Abort(xid)
}

func (v *VmImpl) getTransaction(xid int64) *Transaction {
	v.lock.RLock()
	defer v.lock.RUnlock()
	if ret, ext := v.activeTrans[xid]; !ext {
		return nil
	} else {
		return ret
	}
}

func (v *VmImpl) checkMvccValid(record Record, xid int64) bool {
	transaction := v.getTransaction(xid)
	if transaction == nil {
		panic("Error occurs when getting transaction struct, it is not an active transaction")
	}
	readView := transaction.rv
	recordXid := record.GetXid()
	// 自己创建的
	if recordXid == xid {
		return true
	}
	if recordXid < readView.minXid {
		return true
	}
	if recordXid >= readView.maxXid {
		return false
	}
	for _, activeId := range readView.active {
		if activeId == recordXid {
			return false
		}
	}
	return true
}

// tryToLockTable
// 尝试获取tbUid锁
// 加锁策略，先自旋获取MaxTryLockCount次锁，如果依旧失败，在tbUid的owner事物的channel上阻塞
func (v *VmImpl) tryToLockTable(xid, tbUid int64) error {
	tryTime := 0 // 尝试获取锁的次数
	var lastOwner int64 = -1
	for true {
		// 成功获取表锁
		locked, last, err := v.lt.AddLock(xid, tbUid, lastOwner)
		if err != nil {
			// 死锁，回滚
			if errors.Is(err, &DeadLockError{}) {
				v.Abort(xid)
				return err
			} else {
				panic(fmt.Sprintf("Error occurs when reading for update, err = %s", err))
			}
		}
		// 加锁成功
		if locked {
			break
		}
		lastOwner = last
		tryTime += 1
		if tryTime == MaxTryLockCount {
			v.parkOnChannel(lastOwner)
		}
	}
	return nil
}

// parkOnChannel
// 在transaction上的channel阻塞
func (v *VmImpl) parkOnChannel(wait int64) {
	if tran := v.getTransaction(wait); tran != nil {
		// wait依旧活跃
		select {
		case <-tran.waiting:
			{
			}
		}
	}
}

// endTransaction
// 结束事物
// 必须获取v的锁
func (v *VmImpl) endTransaction(xid int64, tran *Transaction) {
	v.lt.RemoveLock(xid)
	delete(v.activeTrans, xid)
	v.changeMinXid(xid)
	// wake up
	close(tran.waiting)
}

func (v *VmImpl) changeMinXid(xid int64) {
	if xid == v.minActiveXid {
		nextXid := xid + 1
		for true {
			if _, ext := v.activeTrans[nextXid]; !ext {
				nextXid += 1
			} else {
				v.minActiveXid = nextXid
				break
			}
		}
	}
}

func NewVersionManager(path string, memory int64, lock *sync.RWMutex, isolationLevel IsolationLevel) VersionManager {
	tm := transactions.NewTransactionManagerImpl(path)
	dm := dataManager.OpenDataManager(path, memory, tm)
	undo := OpenUndoLog(path, &sync.Mutex{})
	lt := NewLockTable()
	log.Printf("[Version Manager] Initialze version manager\n")
	return &VmImpl{
		dm: dm, tm: tm, undo: undo, lock: lock,
		activeTrans: map[int64]*Transaction{},
		lt:          lt,
	}
}
