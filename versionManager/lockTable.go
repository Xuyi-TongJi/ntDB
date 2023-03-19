package versionManager

import "sync"

// LockTable 记录当前VersionManager的锁状态
// 用于死锁检测

type LockTable interface {
	AddLock(xid, tbUid, lastOwner int64) (bool, int64, error) // 事物xid对tb加锁
	RemoveLock(xid int64)                                     // remove事物xid上的所有锁
	checkDeadLock() bool
}

type LockTableImpl struct {
	locks      map[int64][]int64 // xid -> tbUid
	lockStatus map[int64]int64   // tbUid -> owner(xid)
	lockEdge   map[int64][]int64 // xid1 -> xid2 (xid2 is waiting for xid1)
	lock       sync.Mutex
}

type DeadLockError struct{}

func (err *DeadLockError) Error() string {
	return "A DeadLock will happen, this transaction must be roll back"
}

// AddLock
// 事物xid对tbUid加锁(表锁)
func (lt *LockTableImpl) AddLock(xid, tbUid, lastOwner int64) (bool, int64, error) {
	lt.lock.Lock()
	defer lt.lock.Unlock()
	// 锁是否有owner
	// 有owner，加锁失败, 检测是否存在循环等待条件
	if _, ext := lt.lockStatus[tbUid]; ext {
		owner := lt.lockStatus[tbUid]
		// owner是本身，发生可重入
		if owner == xid {
			return true, owner, nil
		}
		// 上次获取锁时，owner未改变, 上一次通过了死锁检测，这一次不需要检测(已经有owner->xid这条边)
		if owner == lastOwner {
			return false, owner, nil
		}
		lt.lockEdge[owner] = append(lt.lockEdge[owner], xid)
		if lt.checkDeadLock() {
			lt.lockEdge[owner] = append(lt.lockEdge[owner][:len(lt.lockEdge[owner])-1])
			return false, owner, &DeadLockError{}
		} else {
			return false, owner, nil
		}
	} else {
		// 没有owner 加锁成功
		lt.lockStatus[tbUid] = xid
		lt.locks[xid] = append(lt.locks[xid], tbUid)
		return true, xid, nil
	}
}

// RemoveLock
// 移除事物xid上的所有锁
func (lt *LockTableImpl) RemoveLock(xid int64) {
	lt.lock.Lock()
	defer lt.lock.Unlock()
	for _, tbUid := range lt.locks[xid] {
		delete(lt.lockStatus, tbUid)
	}
	delete(lt.lockEdge, xid)
	delete(lt.locks, xid)
}

// checkDeadLock
// 死锁检测
// 拓扑排序
func (lt *LockTableImpl) checkDeadLock() bool {
	in := map[int64]int{}
	all := map[int64]struct{}{}
	// x1 -> x2 x2等待x1的释放
	for x1, e := range lt.lockEdge {
		for _, x2 := range e {
			in[x2] += 1
			all[x2] = struct{}{}
		}
		all[x1] = struct{}{}
	}
	var q []int64
	for x, e := range lt.lockEdge {
		if len(e) > 0 && in[x] == 0 {
			q = append(q, x)
		}
	}
	cnt := 0
	for len(q) > 0 {
		curr := q[0]
		q = append(q[1:])
		cnt += 1
		for _, to := range lt.lockEdge[curr] {
			in[to] -= 1
			if in[to] == 0 {
				q = append(q, to)
			}
		}
	}
	return cnt == len(all)
}

func NewLockTable() LockTable {
	return &LockTableImpl{
		locks:      map[int64][]int64{},
		lockStatus: map[int64]int64{},
		lockEdge:   map[int64][]int64{},
	}
}
