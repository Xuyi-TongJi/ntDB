package versionManager

import (
	"errors"
	"fmt"
	"myDB/dataManager"
	"myDB/transactions"
	"sync"
	"time"
)

type VersionManager interface {
	Read(xid, uid int64) Record
	ReadForUpdate(xid, uid, tbUid int64) (Record, error)         // ReadForUpdate 当前读
	Update(xid, uid, tbUid int64, newData []byte) (int64, error) // Update 更新 返回更新后的uid
	Insert(xid int64, data []byte, tbUid int64) (int64, error)   // Insert 返回插入位置(uid)
	Delete(xid, uid, tbUid int64) error
	CreateReadView(xid int64) *ReadView // 创建读视图

	Begin(level IsolationLevel, autoCommitted bool) int64
	Commit(xid int64)
	Abort(xid int64)
}

type VmImpl struct {
	dm           dataManager.DataManager
	tm           transactions.TransactionManager
	undo         Log
	activeTrans  map[int64]*Transaction
	nextXid      int64 // 下一个事物的xid
	minActiveXid int64 // 当前活跃的事物最小xid
	lt           LockTable
	lock         *sync.RWMutex
}

const (
	MetaDataTbUid     int64 = -1
	SleepTimeUnLocked       = 20 * time.Millisecond
)

// Read
// 快照读 MVCC
// 没有写权限
// 可能返回nil
func (v *VmImpl) Read(xid, uid int64) Record {
	transaction := v.getTransaction(xid)
	if transaction.level == ReadCommitted {
		transaction.rv = v.CreateReadView(xid)
	}
	di := v.dm.Read(uid) // DataItem
	if di == nil {
		return nil
	}
	record := DefaultRecordFactory.NewRecord(di.GetData(), di, v, uid, v.undo, false)
	// 超级事物创建的
	if record.GetXid() == transactions.SuperXID {
		return record
	}
	for record != nil && !v.checkMvccValid(record, xid) {
		record = DefaultRecordFactory.NewSnapShot(record.GetData(), v.undo)
	}
	return record
}

// ReadForUpdate
// 当前读
// 具有写权限
// 真正执行更新操作的是TBM(表和字段管理)
func (v *VmImpl) ReadForUpdate(xid, uid, tbUid int64) (Record, error) {
	_ = v.getTransaction(xid) // check valid
	if err := v.tryToLockTable(xid, tbUid); err != nil {
		return nil, err
	}
	di := v.dm.Read(uid)
	if di == nil {
		return nil, nil
	}
	return DefaultRecordFactory.NewRecord(di.GetData(), di, v, uid, v.undo, true), nil
}

func (v *VmImpl) Update(xid, uid, tbUid int64, newData []byte) (int64, error) {
	tran := v.getTransaction(xid) // check valid
	// readForUpdate 获取表锁
	if record, err := v.ReadForUpdate(xid, uid, tbUid); err != nil {
		return -1, err
	} else {
		// undoLog
		rollback := v.undo.Log(record.GetRaw())
		newRecordRaw := WrapRecordRaw(newData, xid, rollback)
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
	// metaData, 不需要获得锁，直接插入
	if tbUid == MetaDataTbUid {
		return v.dm.Insert(xid, data), nil
	}
	if err := v.tryToLockTable(xid, tbUid); err != nil {
		return -1, err
	}
	// xid已经取得表锁，不需要继续加v锁
	uid := v.dm.Insert(xid, data)
	tran.AddInsert(uid)
	return uid, nil
}

func (v *VmImpl) Delete(xid, uid, tbUid int64) error {
	tran := v.getTransaction(xid) // check valid
	// metaData, 不需要获得锁，直接删除
	if tbUid == MetaDataTbUid {
		v.dm.Delete(xid, uid)
	}
	if err := v.tryToLockTable(xid, tbUid); err != nil {
		return err
	}
	// xid已经取得表锁，不需要继续加v锁
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
func (v *VmImpl) Begin(level IsolationLevel, autoCommitted bool) int64 {
	v.lock.Lock()
	defer v.lock.Unlock()
	xid := v.tm.Begin()
	trans := NewTransaction(xid, level, autoCommitted, v)
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
	v.checkXidActive(xid) // check Valid
	v.lock.Lock()
	defer v.lock.Unlock()
	v.lt.RemoveLock(xid)
	delete(v.activeTrans, xid)
	v.changeMinXid(xid)
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
	v.lt.RemoveLock(xid)
	delete(v.activeTrans, xid)
	v.changeMinXid(xid)
	// tm
	v.tm.Abort(xid)
}

func (v *VmImpl) getTransaction(xid int64) *Transaction {
	v.lock.RLock()
	defer v.lock.RUnlock()
	if ret, ext := v.activeTrans[xid]; !ext {
		panic("Error occurs when getting transaction, this is an inactive transaction")
	} else {
		return ret
	}
}

// checkXidActive
// 检测
func (v *VmImpl) checkXidActive(xid int64) {
	if _, ext := v.activeTrans[xid]; !ext {
		panic("Error occurs when getting transaction, this is an inactive transaction")
	}
}

func (v *VmImpl) checkMvccValid(record Record, xid int64) bool {
	transaction := v.getTransaction(xid)
	readView := transaction.rv
	recordXid := record.GetXid()
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

func (v *VmImpl) tryToLockTable(xid, tbUid int64) error {
	var lastOwner int64 = -1
	for true {
		// 成功获取表锁
		locked, last, err := v.lt.AddLock(xid, tbUid, lastOwner)
		if err != nil {
			// 死锁， 回滚
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
		time.Sleep(SleepTimeUnLocked)
	}
	return nil
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

func NewVersionManager(path string, memory int64, lock *sync.RWMutex) VersionManager {
	tm := transactions.NewTransactionManagerImpl(path)
	dm := dataManager.OpenDataManager(path, memory, tm)
	undo := OpenUndoLog(path, &sync.Mutex{})
	lt := NewLockTable()
	return &VmImpl{
		dm: dm, tm: tm, undo: undo, lock: lock,
		activeTrans: map[int64]*Transaction{},
		lt:          lt,
	}
}
