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
	ReadForUpdate(xid, uid, tbUid int64) (Record, error)
	Insert(xid int64, data []byte, tbUid int64) (int64, error)
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
	MetaDataTbUid     int64         = -1
	SleepTimeUnLocked time.Duration = 50 * time.Millisecond
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

// Insert
// 向DataManager插入数据
// 可以插入元数据（表，字段信息），也可以插入（索引，数据）
// 当插入元数据时，tbUid == -1
// 返回DataItem的uid
func (v *VmImpl) Insert(xid int64, data []byte, tbUid int64) (int64, error) {
	v.lock.Lock()
	defer v.lock.Unlock()
	// metaData, 不需要获得锁，直接插入
	if tbUid == MetaDataTbUid {
		return v.dm.Insert(xid, data), nil
	}
	if err := v.tryToLockTable(xid, tbUid); err != nil {
		return -1, err
	}
	return v.dm.Insert(xid, data), nil
}

func (v *VmImpl) Delete(xid, uid, tbUid int64) error {
	v.lock.Lock()
	defer v.lock.Unlock()
	// metaData, 不需要获得锁，直接删除
	if tbUid == MetaDataTbUid {
		v.dm.Delete(xid, uid)
	}
	if err := v.tryToLockTable(xid, tbUid); err != nil {
		return err
	}
	v.dm.Delete(xid, uid)
	return nil
}

func (v *VmImpl) CreateReadView(xid int64) *ReadView {
	v.lock.RLock()
	defer v.lock.RUnlock()
	var active []int64
	for xid, _ := range v.activeTrans {
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
	v.lock.Lock()
	defer v.lock.Unlock()
	//TODO implement me
	panic("implement me")
}

// Abort
// 回滚事物
// 释放该事物持有的所有锁
// 更新vm状态
func (v *VmImpl) Abort(xid int64) {
	v.lock.Lock()
	defer v.lock.Unlock()
	//TODO implement me
	panic("implement me")
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
	for true {
		// 成功获取表锁
		locked, err := v.lt.AddLock(xid, tbUid)
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
		time.Sleep(SleepTimeUnLocked)
	}
	return nil
}

func NewVersionManager(path string, memory int64, lock *sync.RWMutex) VersionManager {
	tm := transactions.NewTransactionManagerImpl(path)
	dm := dataManager.OpenDataManager(path, memory, tm)
	undo := OpenUndoLog(path, &sync.Mutex{})
	// TODO
	lt := NewLockTable()
	return &VmImpl{
		dm: dm, tm: tm, undo: undo, lock: lock,
		activeTrans: map[int64]*Transaction{},
		lt:          lt,
	}
}
