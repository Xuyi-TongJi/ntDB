package util

import (
	"math/rand"
	"time"
)

const (
	maxLevel int = 20
)

func init() {
	rand.Seed(time.Now().Unix())
}

type skipListNode struct {
	val  any
	next [maxLevel]*skipListNode
}

type SkipList struct {
	root            *skipListNode
	compareFunction func(any, any) int
}

func NewSkipList(f func(any, any) int) *SkipList {
	return &SkipList{
		root: &skipListNode{struct{}{},
			[maxLevel]*skipListNode{}},
	}
}

// BinarySearch
// 二分查找>=target的第一个元素
func (list *SkipList) BinarySearch(target any) any {
	ans := list.find(target)
	if ans[0].next[0] == nil {
		return nil
	}
	return ans[0].next[0].val
}

func (list *SkipList) Add(target any) {
	ans := list.find(target)
	newNode := &skipListNode{target, [maxLevel]*skipListNode{}}
	for i := 0; i < maxLevel; i++ {
		newNode.next[i] = ans[i]
		ans[i].next[i] = newNode
		if rand.Intn(2) == 0 {
			break
		}
	}
}

func (list *SkipList) Remove(target any) {
	ans := list.find(target)
	if ans[0].next[0] != nil && list.compareFunction(ans[0].next[0].val, target) == 0 {
		for i := 0; i < maxLevel; i++ {
			if ans[i].next[i] != nil && list.compareFunction(ans[0].next[0].val, target) == 0 {
				ans[i].next[i] = ans[i].next[i].next[i]
			} else {
				break
			}
		}
	}
}

func (list *SkipList) find(target any) [maxLevel]*skipListNode {
	ans := [maxLevel]*skipListNode{}
	curr := list.root
	for i := maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && list.compareFunction(curr.next[i].val, target) == -1 {
			curr = curr.next[i]
		}
		ans[i] = curr
	}
	return ans
}
