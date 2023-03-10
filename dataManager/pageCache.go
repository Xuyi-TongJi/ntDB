package dataManager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	defaultPageFactory pageFactory
)

func init() {
	defaultPageFactory = pageFactoryImpl{}
}

// PageCache
// 基于页面Page的缓存接口

type PageCache interface {
	NewPage(data []byte, pageType PageType) int64
	GetPage(pageId int64) (Page, error)
	ReleasePage(page Page) error
	TruncateDataSource(maxPageNumbers int64) error
	GetPageNumbers() int64
	Close()
	DoFlush(page Page) // 直接刷新到数据源
}

// Implementation
// PageCache实现类
// 实现PageCache接口
// 桥接模式

type PageCacheImpl struct {
	// implemented by PageCache
	pool        BufferPool
	ds          DataSource   // only used for NewPage-> doFlush method
	lock        *sync.Mutex  // protect the NewPage/ GetPage/ ReleasePage, the only global lock of the page cache system
	pageNumbers atomic.Int64 // the total page numbers in the currentFile
}

func (p *PageCacheImpl) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := p.pool.Close(); err != nil {
		panic(err)
	}
	if err := p.ds.Close(); err != nil {
		panic(err)
	}
}

// NewPage 新建一个页，并写入数据源
func (p *PageCacheImpl) NewPage(data []byte, pt PageType) int64 {
	p.lock.Lock()
	defer p.lock.Unlock()
	if int64(len(data)) > PageSize-SzUsed-SzPageType {
		panic("Data length overflow when creating a new page")
	}
	newPage := defaultPageFactory.newPage(p.ds, p.pageNumbers.Load()+1, p, pt)
	if err := newPage.Append(data); err != nil {
		panic(fmt.Sprintf("Error occurs when creating a page, err = %s\n", err))
	}
	p.pageNumbers.Add(1)
	p.DoFlush(newPage)
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
	page := defaultPageFactory.newPage(p.ds, pageId, p, -1)
	if result, err := p.pool.Get(page); err != nil {
		return nil, err
	} else {
		pg := result.(Page)
		return pg, nil
	}
}

// ReleasePage 释放对Page的引用，用于内存淘汰
func (p *PageCacheImpl) ReleasePage(page Page) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.pool.Release(page)
}

// TruncateDataSource 清空DataSource, 并预留maxPageNumbers个页的空间
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

// DoFlush
// must take the lock first(private method)
// flush the page into data source, any error will panic
// ONLY WRITE TO THE OS CACHE
func (p *PageCacheImpl) DoFlush(page Page) {
	if err := p.ds.FlushBackToDataSource(page); err != nil {
		panic(err)
	}
}

// Page Factory

type pageFactory interface {
	newPage(ds DataSource, pageId int64, pc PageCache, pageType PageType) Page
}

type pageFactoryImpl struct{}

// 工厂方法
// extensible
func (p pageFactoryImpl) newPage(ds DataSource, pageId int64, pc PageCache, pageType PageType) Page {
	switch ds.(type) {
	case *FileSystemDataSource:
		data := make([]byte, PageSize)
		buf := bytes.NewBuffer([]byte{})
		_ = binary.Write(buf, binary.BigEndian, int32(SzPageType+SzUsed))
		copy(data[0:SzUsed], buf.Bytes())
		buf = bytes.NewBuffer([]byte{})
		_ = binary.Write(buf, binary.BigEndian, int32(pageType))
		copy(data[SzUsed:SzUsed+SzPageType], buf.Bytes())
		return &PageImpl{
			pageId: pageId, dirty: false, pc: pc, data: data,
		}
	default:
		panic("Invalid dataSource type\n")
	}
}

func NewPageCacheRefCountFileSystemImpl(maxRecourse uint32, path string, lock *sync.Mutex) PageCache {
	this := &PageCacheImpl{lock: lock}
	ds := NewFileSystemDataSource(path, lock)
	length := ds.GetDataLength()
	this.pageNumbers.Store(length / PageSize)
	this.ds = ds
	bufferPool := NewRefCountBufferPool(maxRecourse, ds, lock)
	this.pool = bufferPool
	if this.pageNumbers.Load() < 1 {
		// set db meta page
		this.NewPage([]byte{}, DbMetaPage)
	}
	return this
}
