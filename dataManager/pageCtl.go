package dataManager

import (
	"fmt"
	"log"
	. "myDB/dataStructure"
	"sync"
)

// PageCtl 页面信息控制器
// 管理PageCache中的空闲页面free/tiny以及脏页面dirties

type PageCtl interface {
	Select(need int64) *PageInfo
	AddPageInfo(pageId, available int64)
	Init(pc PageCache)
}

type PageInfo struct {
	PageId    int64
	Available int64
}

const (
	THRESHOLD     int64 = 128
	TinyTHRESHOLD       = THRESHOLD / 4
	INTERVALS           = PageSize / THRESHOLD //
	OMITTED       int64 = 8                    // 剩余空间小于8的内存页都会被弃用
)

// PageCtlImpl
// 将每个区间拆分成64个小区间
type PageCtlImpl struct {
	free     [INTERVALS]*LinkedList // [32,127], [127,255]... (链表)
	locks    [INTERVALS]sync.Mutex
	tiny     *SkipList // 剩余空间<32Bytes且>=8的页(跳表)
	tinyLock sync.Mutex
	pc       PageCache
}

func NewPageCtl(pc PageCache) PageCtl {
	var pi [INTERVALS]*LinkedList
	f := func(a any, b any) int {
		x, y := a.(*PageInfo).Available, b.(*PageInfo).Available
		if x == y {
			return 0
		} else if x < y {
			return -1
		} else {
			return 1
		}
	}
	for i := int64(0); i < INTERVALS; i++ {
		pi[i] = NewLinkedList(f)
	}
	ctl := &PageCtlImpl{free: pi, tiny: NewSkipList(f), pc: pc}
	return ctl
}

// Select
// 为need字节空间选择合适的页并删除
// Select and remove 操作必须是原子的
func (pi *PageCtlImpl) Select(need int64) *PageInfo {
	if need <= 0 {
		panic("Illegal page cache application operation\n")
	}
	if need > PageSize {
		panic("Applying for overflowed page size\n")
	}
	var intervalNum int64
	if need < TinyTHRESHOLD {
		// < 32Bytes
		// find a page that is available
		if result := pi.selectTinyFast(need); result != nil {
			return result
		} else {
			intervalNum = 0
		}
	} else {
		intervalNum = need / THRESHOLD
	}
	if intervalNum != INTERVALS-1 {
		intervalNum += 1
	}
	for ; intervalNum < INTERVALS; intervalNum += 1 {
		if result := pi.selectAndRemove(need, intervalNum); result != nil {
			return result
		}
	}
	return nil
}

func (pi *PageCtlImpl) selectTinyFast(need int64) *PageInfo {
	pi.tinyLock.Lock()
	defer pi.tinyLock.Unlock()
	if need < TinyTHRESHOLD {
		result := pi.tiny.BinarySearch(&PageInfo{-1, need})
		if result != nil {
			pi.tiny.Remove(result)
			return result.(*PageInfo)
		}
	}
	return nil
}

func (pi *PageCtlImpl) selectAndRemove(need, intervalId int64) *PageInfo {
	pi.locks[intervalId].Lock()
	defer pi.locks[intervalId].Unlock()
	toFind := &PageInfo{-1, need}
	if result := pi.free[intervalId].FindGtAndRemove(toFind); result != nil {
		return result.(*PageInfo)
	}
	return nil
}

// AddPageInfo 添加一个具有available可用空间的页
// 注意该空间不一定等于页的大小(PageSize)
func (pi *PageCtlImpl) AddPageInfo(pageId int64, available int64) {
	if available < OMITTED {
		return
	}
	if available < TinyTHRESHOLD {
		pi.tinyLock.Lock()
		defer pi.tinyLock.Unlock()
		pi.tiny.Add(&PageInfo{pageId, available})
	} else {
		intervalId := available / THRESHOLD
		pi.locks[intervalId].Lock()
		defer pi.locks[intervalId].Unlock()
		pi.free[intervalId].AddLast(&PageInfo{pageId, available})
	}
}

// Init 初始化PageCtlImpl
// 将所有页都读入buffer, 并更新free spaces
func (pi *PageCtlImpl) Init(pc PageCache) {
	pn := pc.GetPageNumbers()
	for i := int64(1); i <= pn; i++ {
		if i == PageNumberDbMeta {
			continue
		}
		if p, err := pc.GetPage(i); err != nil {
			panic(fmt.Sprintf("Error occurs when getting pages, err = %s\n", err))
		} else {
			if p.IsDataPage() {
				pi.AddPageInfo(p.GetId(), p.GetFree())
			}
			if err = pc.ReleasePage(p); err != nil {
				panic(fmt.Sprintf("Error occurs when releasing pages, err = %s\n", err))
			}
		}
	}
	log.Printf("[DataManager] Initialize page control\n")
}
