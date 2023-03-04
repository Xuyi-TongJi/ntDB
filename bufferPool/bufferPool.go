package bufferPool

type BufferPool interface {
	Get(key int64) (interface{}, error) // 获取缓存,如果不在内存中，则发起IO请求
	Release(key int64) error            // 释放缓存
	Close() error                       // 安全关闭缓冲区
}
