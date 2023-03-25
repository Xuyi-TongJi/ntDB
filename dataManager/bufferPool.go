package dataManager

type PoolObj interface {
	IsDirty() bool
	SetDirty(dirty bool)
	SetData(data []byte)
	GetId() int64
	GetDataSize() int64
	GetData() []byte
}

type BufferPool interface {
	Get(key PoolObj) (PoolObj, error) // 获取缓存,如果不在内存中，则发起IO请求
	Release(key PoolObj) error        // 释放缓存
	Close() error                     // 安全关闭缓冲区
}
