package dataManager

import (
	"os"
	"sync"
)

type DataSource interface {
	GetFromDataSource(obj PoolObj) ([]byte, error)
	FlushBackToDataSource(obj PoolObj) error
	Truncate(size int64) error
	Close() error
}

// FileSystemDataSource

type FileSystemDataSource struct {
	file *os.File
	lock *sync.Mutex
}

type FileSystemObj interface {
	PoolObj
	GetOffset() int64
}

func NewFileSystemDataSource(path string, lock *sync.Mutex) DataSource {
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	return &FileSystemDataSource{
		file: f,
	}
}

// GetFromDataSource
// 缓存未命中的执行逻辑
// 从文件系统中读取, 返回读到的字节数组
// param: offset, size
func (ch *FileSystemDataSource) GetFromDataSource(obj PoolObj) ([]byte, error) {
	fso, ok := obj.(FileSystemObj)
	if !ok {
		panic("File System Data Source illegal param\n")
	}
	offset, size := fso.GetOffset(), fso.GetDataSize()
	buf := make([]byte, size)
	_, err := ch.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// FlushBackToDataSource
// 缓存淘汰时写回磁盘
// param: offset, size, data
func (ch *FileSystemDataSource) FlushBackToDataSource(obj PoolObj) error {
	fso, ok := obj.(FileSystemObj)
	if !ok {
		panic("File System Data Source illegal param\n")
	}
	_, err := ch.file.WriteAt(fso.GetData(), fso.GetOffset())
	return err
}

func (ch *FileSystemDataSource) Truncate(size int64) error {
	return ch.file.Truncate(size)
}

func (ch *FileSystemDataSource) Close() error {
	return ch.file.Close()
}
