package bufferPool

import (
	"log"
	"sync"
	"time"
)

// RefCountBufferPoolImpl 基于RefCount 实现BufferPool
type RefCountBufferPoolImpl struct {
	cache       map[int64]any
	refCount    map[int64]uint32
	caching     map[int64]struct{} // 正在进行IO请求的key
	maxRecourse uint32             // bufferPool最大支持的缓存cacheId个数
	count       uint32             // 目前内存中的cacheId个数
	lock        sync.Mutex
}

func NewRefCountBufferPoolImpl(maxRecourse uint32) BufferPool {
	return &RefCountBufferPoolImpl{
		cache:       map[int64]any{},
		refCount:    map[int64]uint32{},
		caching:     map[int64]struct{}{},
		maxRecourse: maxRecourse,
		count:       0,
	}
}

func (p *RefCountBufferPoolImpl) Get(key int64) (any, error) {
	p.lock.Lock()
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
	// before ask for I/O request
	if p.count+1 > p.maxRecourse {
		p.lock.Unlock()
		panic("Buffer pool out of memory\n")
	}
	p.count += 1
	p.caching[key] = struct{}{}
	p.lock.Unlock()
	// ask for I/O request
	obj, err := p.getToCache(key)
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
	p.cache[key] = obj
	//fmt.Printf("obj-->%d, refCount-->%d\n", obj.(int64), p.refCount[key])
	p.lock.Unlock()
	return obj, nil
}

func (p *RefCountBufferPoolImpl) Release(key int64) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	count, ext := p.refCount[key]
	if !ext {
		panic("Try to release a key which is not in buffer pool\n")
	}
	count -= 1
	if count == 0 {
		obj := p.cache[key]
		if err := p.releaseFromCache(obj); err != nil {
			return err
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
	p.lock.Lock()
	defer p.lock.Unlock()
	for key, obj := range p.cache {
		if err := p.releaseFromCache(obj); err != nil {
			return err
		}
		delete(p.cache, key)
		delete(p.refCount, key)
		p.count -= 1
	}
	return nil
}

// 引用计数将为0时，执行回写数据的逻辑
func (p *RefCountBufferPoolImpl) releaseFromCache(obj any) error {
	return nil
}

// 缓存未命中时，发起IO请求读入缓存
func (p *RefCountBufferPoolImpl) getToCache(key int64) (any, error) {
	// TODO
	return key, nil
}

// Debug only for debug
func (p *RefCountBufferPoolImpl) Debug() {
	log.Println("Ref count cache")
	for k, v := range p.refCount {
		log.Printf("%d, %d\n", k, v)
	}
	log.Println("Cache")
	for k, v := range p.cache {
		log.Printf("%d, %d\n", k, v.(int64))
	}
}
