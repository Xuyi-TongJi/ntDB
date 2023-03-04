package dataManager

import (
	. "myDB/bufferPool"
	"sync"
)

// 页及页缓存及其实现

const (
	PageSize int64 = 8192 // 8K bytes
)

type Page interface {
	PoolObj
	Lock()
	Unlock()
}

type PageFileSystemImpl struct {
	lock   sync.Mutex
	data   []byte
	dirty  bool
	pageId int64
	pc     PageCache // 每个Page组合一个PageCache，可以在操作页面时对页面缓存进行操作
}

func (p *PageFileSystemImpl) Lock() {
	p.lock.Lock()
}

func (p *PageFileSystemImpl) Unlock() {
	p.lock.Unlock()
}

func (p *PageFileSystemImpl) IsDirty() bool {
	return p.dirty
}

func (p *PageFileSystemImpl) SetDirty(dirty bool) {
	p.dirty = dirty
}

func (p *PageFileSystemImpl) GetId() int64 {
	return p.pageId
}

func (p *PageFileSystemImpl) GetData() []byte {
	return p.data
}

func (p *PageFileSystemImpl) GetOffset() int64 {
	return (p.pageId - 1) * PageSize
}

func (p *PageFileSystemImpl) GetDataSize() int64 {
	return PageSize
}

func (p *PageFileSystemImpl) SetData(data []byte) {
	p.data = data
}
