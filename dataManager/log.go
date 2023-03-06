package dataManager

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
)

// 日志系统实现
// 迭代器模式

type Log interface {
	Log(data []byte) // 记录下一条log
	Close()
	Next() []byte // 迭代器获得下一条log data
	Reset()       // 重置迭代器指针
}

const (
	SEED       int    = 1331
	MOD        int    = 998244353
	LogSuffix  string = ".log"
	SzCheckSum int64  = 4
	SzData     int64  = 4
)

type RedoLog struct {
	file     *os.File
	checkSum int32
	lock     sync.Mutex
	offset   int64 // current pointer used for iterator
}

func (log *RedoLog) Log(data []byte) {
	log.lock.Lock()
	defer log.lock.Unlock()
	logWrap := log.wrapLog(data)
	// write
	stat, _ := log.file.Stat()
	if _, err := log.file.WriteAt(logWrap, stat.Size()); err != nil {
		panic(err)
	}

}

func (log *RedoLog) Close() {
	log.lock.Lock()
	defer log.lock.Unlock()
	if err := log.file.Close(); err != nil {
		panic(err)
	}
}

// Next 迭代器模式
// nextUnlock 的加锁实现
func (log *RedoLog) Next() []byte {
	log.lock.Lock()
	defer log.lock.Unlock()
	return log.nextUnlock()
}

func (log *RedoLog) Reset() {
	log.offset = SzCheckSum
}

// init
// 根据redoLog文件恢复现场参数(offset, checkSum)
func (log *RedoLog) init() {
	// read checkSum
	stat, _ := log.file.Stat()
	if stat.Size() < SzCheckSum {
		panic("Invalid checkSum length when initializing redo log\n")
	}
	buf := make([]byte, SzCheckSum)
	if _, err := log.file.ReadAt(buf, 0); err != nil {
		panic(fmt.Sprintf("Error occuring when initializing redo log, %s\n", err))
	}
	log.checkSum = int32(binary.BigEndian.Uint32(buf))
	log.removeTail() // set offset
}

// removeTail
// 移除log中上次关闭未写完的部分
// only in init method
func (log *RedoLog) removeTail() {
	log.Reset()
	var checkedCheckSum int32 = 0
	for true {
		nextLogData := log.nextUnlock()
		if nextLogData == nil {
			break
		}
		checkedCheckSum = calcCheckSum(int(checkedCheckSum), nextLogData)
	}
	if checkedCheckSum != log.checkSum {
		panic("Invalid redo log file\n")
	}
	// truncate
	log.truncate(log.offset)
	log.Reset() // roll back the pointer
}

// truncate 截断文件
func (log *RedoLog) truncate(size int64) {
	if err := log.file.Truncate(size); err != nil {
		panic(err)
	}
}

// nextUnlock
// 仅适用于 removeTail 方法
// return the data of next log
// if !hasNext or the next log is invalid then return nil
func (log *RedoLog) nextUnlock() (data []byte) {
	// next log is invalid
	stat, _ := log.file.Stat()
	totSize := stat.Size()
	if log.offset+SzData+SzCheckSum > totSize {
		return nil
	}
	buffer := make([]byte, SzData)
	if _, err := log.file.ReadAt(buffer, log.offset); err != nil {
		panic(err)
	}
	dataSize := int64(binary.BigEndian.Uint32(buffer))
	if log.offset+SzData+SzCheckSum+dataSize > totSize {
		return nil
	}
	data = make([]byte, dataSize)
	if _, err := log.file.ReadAt(data, log.offset+SzData+SzCheckSum); err != nil {
		panic(err)
	}
	// 当且仅当完整读完一条log时，更改offset
	log.offset += SzData + SzCheckSum + dataSize
	return
}

// 将size...checkSum...data 包装成一条log
func (log *RedoLog) wrapLog(data []byte) []byte {
	checkSum := calcCheckSum(0, data)
	size := len(data)
	buffer1 := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer1, binary.BigEndian, int32(size))
	buffer2 := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer2, binary.BigEndian, checkSum)
	// size...checkSum...data
	ret := make([]byte, SzCheckSum+SzData+int64(len(data)))
	copy(ret[0:SzData], buffer1.Bytes())
	copy(ret[SzData:SzCheckSum+SzData], buffer2.Bytes())
	copy(ret[SzCheckSum+SzData:], data)
	return ret
}

// Crash Recovery

// TODO

// Create

func CreateRedoLog(path string) Log {
	file, err := os.Create(path + LogSuffix)
	if err != nil {
		panic(err)
	}
	buffer := bytes.NewBuffer(make([]byte, 0))
	_ = binary.Write(buffer, binary.BigEndian, int32(0))
	// 4 bytes checkSum
	if _, err := file.Write(buffer.Bytes()); err != nil {
		panic(err)
	}
	redoLog := &RedoLog{
		file:     file,
		checkSum: 0,
	}
	redoLog.Reset()
	return redoLog
}

func OpenRedoLog(path string) Log {
	file, err := os.OpenFile(path+LogSuffix, os.O_RDWR, 0666)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return CreateRedoLog(path)
	} else if err != nil {
		panic(err)
	}
	redoLog := &RedoLog{
		file: file,
	}
	redoLog.init()
	return redoLog
}

func calcCheckSum(checkSum int, data []byte) int32 {
	for _, b := range data {
		checkSum = (checkSum + SEED*int(b)%MOD) % MOD
	}
	return int32(checkSum)
}
