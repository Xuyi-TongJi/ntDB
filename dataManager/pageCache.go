package dataManager

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// PageCache
// 基于页面Page的缓存接口

type PageCache interface {
	NewPage(data []byte) int64
	GetPage(pageId int64) (Page, error)
	ReleasePage(page Page) error
	TruncateDataSource(maxPageNumbers int64) error
	GetPageNumbers() int64
	Close() error
}

// Implementation
// PageCache实现类
// 实现PageCache接口
// 组合模式

type PageCacheImpl struct {
	// implemented by PageCache
	pool        BufferPool
	ds          DataSource   // only used for NewPage-> doFlush method
	lock        sync.Mutex   // protect the NewPage/ GetPage/ ReleasePage, the only global lock of the page cache system
	pageNumbers atomic.Int64 // the total page numbers in the currentFile
}

func (p *PageCacheImpl) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := p.pool.Close(); err != nil {
		return err
	}
	return p.ds.Close()
}

// NewPage 新建一个页，并写入数据源
func (p *PageCacheImpl) NewPage(data []byte) int64 {
	p.lock.Lock()
	defer p.lock.Unlock()
	newPage := p.newPageByDsType(p.ds, p.pageNumbers.Load()+1)
	newPage.SetData(data)
	p.doFlush(newPage)
	p.pageNumbers.Add(1)
	return p.pageNumbers.Load()
}

// GetPage 缓存未命中时的页面获取策略
// 并发安全由BufferPool实现
// 将数据源中的数据封装成Page
func (p *PageCacheImpl) GetPage(pageId int64) (Page, error) {
	if !p.checkKeyValid(pageId) {
		panic("Invalid page id\n")
	}
	// 组装空Page
	page := p.newPageByDsType(p.ds, pageId)
	if result, err := p.pool.Get(page); err != nil {
		return nil, err
	} else {
		return result.(Page), nil
	}
}

// ReleasePage 释放对Page的引用，用于内存淘汰
func (p *PageCacheImpl) ReleasePage(page Page) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.pool.Release(page)
}

// TruncateDataSource 清空文件, 并预留maxPageNumbers个页的空间
// if ds doesn't support Truncation, then panic
func (p *PageCacheImpl) TruncateDataSource(maxPageNumbers int64) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := p.ds.Truncate(maxPageNumbers * PageSize); err != nil {
		return err
	}
	p.pageNumbers.Add(maxPageNumbers)
	return nil
}

func (p *PageCacheImpl) GetPageNumbers() int64 {
	return p.pageNumbers.Load()
}

// This method is only in file System
func (p *PageCacheImpl) getPageOffset(pageId int64) int64 {
	return (pageId - 1) * PageSize
}

func (p *PageCacheImpl) checkKeyValid(pageId int64) bool {
	return pageId <= p.pageNumbers.Load() && pageId > 0
}

// doFlush
// must take the lock first(private method)
// flush the page into data source, any error will panic
func (p *PageCacheImpl) doFlush(page Page) {
	pageId := page.GetId()
	if !p.checkKeyValid(pageId) {
		panic(fmt.Sprintf("Invalid page id %d\n", pageId))
	}
	if err := p.ds.FlushBackToDataSource(page); err != nil {
		panic(err)
	}
}

// NewPageByDsType
// extensible
func (p *PageCacheImpl) newPageByDsType(source DataSource, pageId int64) Page {
	switch source.(type) {
	case *FileSystemDataSource:
		return &PageFileSystemImpl{
			pageId: pageId,
			dirty:  false,
			pc:     p,
		}
	default:
		panic("Invalid dataSource type\n")
	}
}

func NewPageCacheRefCountFileSystemImpl(maxRecourse uint32, path string) PageCache {
	this := &PageCacheImpl{}
	ds := NewFileSystemDataSource(path, &this.lock)
	this.ds = ds
	bufferPool := NewRefCountBufferPool(maxRecourse, ds, &this.lock)
	this.pool = bufferPool
	return this
}
