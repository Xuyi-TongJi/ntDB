package dataManager

import (
	. "myDB/dataManager"
	. "myDB/dataSource"
	"sync"
	"time"
)

// RefCountBufferPoolImpl 基于RefCount 实现BufferPool
type RefCountBufferPoolImpl struct {
	cache       map[int64]PoolObj
	refCount    map[int64]uint32
	caching     map[int64]struct{} // 正在进行IO请求的key
	maxRecourse uint32             // bufferPool最大支持的缓存cacheId个数
	count       uint32             // 目前内存中的cacheId个数
	ds          DataSource
	lock        *sync.Mutex // 与PageCache共用一把锁
}

func NewRefCountBufferPool(maxRecourse uint32, ds DataSource, lock *sync.Mutex) BufferPool {
	return &RefCountBufferPoolImpl{
		cache:       map[int64]PoolObj{},
		refCount:    map[int64]uint32{},
		caching:     map[int64]struct{}{},
		maxRecourse: maxRecourse,
		count:       0,
		ds:          ds,
		lock:        lock,
	}
}

func (p *RefCountBufferPoolImpl) Get(obj PoolObj) (PoolObj, error) {
	p.lock.Lock()
	key := obj.GetId()
	for true {
		if _, ext := p.caching[key]; ext {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// already in cache
		if obj, ext := p.cache[key]; ext {
			p.refCount[key] += 1
			p.lock.Unlock()
			return obj, nil
		} else {
			break
		}
	}
	// before ask for data source
	if p.count+1 > p.maxRecourse {
		//p.lock.Unlock()
		panic("Buffer pool out of memory\n")
	}
	p.count += 1
	p.caching[key] = struct{}{}
	p.lock.Unlock()
	// get from datasource
	data, err := p.ds.GetFromDataSource(obj)
	if err != nil {
		p.lock.Lock()
		p.count -= 1
		delete(p.caching, key)
		p.lock.Unlock()
		return nil, err
	}
	// put to cache
	p.lock.Lock()
	delete(p.caching, key)
	p.refCount[key] += 1
	obj.SetData(data)
	p.cache[key] = obj
	p.lock.Unlock()
	return obj, nil
}

func (p *RefCountBufferPoolImpl) Release(obj PoolObj) error {
	key := obj.GetId()
	count, ext := p.refCount[key]
	if !ext {
		panic("Try to release a key which is not in buffer pool\n")
	}
	count -= 1
	if count == 0 {
		if obj.IsDirty() {
			if err := p.ds.FlushBackToDataSource(obj); err != nil {
				return err
			}
		}
		delete(p.refCount, key)
		delete(p.cache, key)
		p.count -= 1
	} else {
		p.refCount[key] = count
	}
	return nil
}

// Close shut up the buffer pool safely
func (p *RefCountBufferPoolImpl) Close() error {
	for key, obj := range p.cache {
		if obj.IsDirty() {
			if err := p.ds.FlushBackToDataSource(obj); err != nil {
				return err
			}
		}
		delete(p.cache, key)
		delete(p.refCount, key)
		p.count -= 1
	}
	return nil
}

// Debug only for debug
func (p *RefCountBufferPoolImpl) Debug() {
	/*log.Println("Ref count cache")
	for k, v := range p.refCount {
		log.Printf("%d, %d\n", k, v)
	}
	log.Println("Cache")
	for k, v := range p.cache {
		//log.Printf("%d, %d\n", k, v.(int64))
	}*/
}
