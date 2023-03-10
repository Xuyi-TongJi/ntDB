package util

type node struct {
	val  any
	prev *node
	next *node
}

type LinkedList struct {
	head            *node
	tail            *node
	size            int
	compareFunction func(any, any) int
}

func NewLinkedList(f func(any, any) int) *LinkedList {
	h, t := &node{nil, nil, nil}, &node{nil, nil, nil}
	h.next = t
	t.prev = h
	return &LinkedList{h, t, 0, f}
}

func (list *LinkedList) AddLast(val any) {
	newNode := &node{val, nil, nil}
	newNode.next = list.tail
	newNode.prev = list.tail.prev
	list.tail.prev.next = newNode
	list.tail.prev = newNode
	list.size += 1
}

func (list *LinkedList) RemoveFirst() any {
	if list.size == 0 {
		return nil
	}
	ret := list.head.next.val
	list.head.next = list.head.next.next
	list.head.next.prev = list.head
	return ret
}

func (list *LinkedList) FindGtAndRemove(target any) any {
	for curr := list.head.next; curr != list.tail; curr = curr.next {
		if list.compareFunction(curr, target) >= 0 {
			curr.removeNode()
			return curr.val
		}
	}
	return nil
}

func (node *node) removeNode() {
	node.prev.next = node.next
	node.next.prev = node.prev
}
