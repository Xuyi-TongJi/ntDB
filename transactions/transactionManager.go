package transactions

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
)

// 事物的状态

const (
	ACTIVE          byte   = 0
	COMMITTED       byte   = 1
	ABORTED         byte   = 2
	SuperXID        int64  = 0      // 超级事物的xid为0，其永远为提交状态
	XidFileSuffix   string = ".xid" // xid文件后缀
	XidStatusSize   int64  = 1      // 每个事物用1个字节(byte)记录
	XidHeaderLength int64  = 8      // 首8个字节用于记录事物的总数
)

// TransactionManager 事物状态管理器
// 记录各个事物的状态
// XID文件为每个事物分配了1字节的空间,用来记录事物的状态
// XID文件的首8个字节用于记录事物的总数
type TransactionManager interface {
	Begin() int64
	Commit(xid int64)
	Abort(xid int64)
	Status(xid int64) byte
	close()
}

type TransactionManagerImpl struct {
	lock       sync.Mutex // 保护新建事物，更新事物状态等操作
	file       *os.File
	xidCounter int64 // xid计数
}

func NewTransactionManagerImpl(path string) TransactionManager {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	t := &TransactionManagerImpl{
		file: file,
	}
	if valid, xid := t.checkXidFile(); !valid {
		panic("Invalid XID File\n")
	} else {
		t.xidCounter = xid
		return t
	}
}

// check XID 文件是否合法
func (t *TransactionManagerImpl) checkXidFile() (bool, int64) {
	stat, err := t.file.Stat()
	if err != nil {
		return false, -1
	}
	if stat.Size() == 0 {
		t.initXidFile()
		return true, 0
	}
	buf := make([]byte, XidHeaderLength)
	_, err = t.file.Read(buf)
	if err != nil {
		return false, -1
	}
	xid := int64(binary.BigEndian.Uint64(buf))
	stat, err = t.file.Stat()
	if err != nil {
		return false, -1
	}
	if t.getXidOffset(xid+1) != stat.Size() {
		return false, -1
	}
	return true, xid
}

func (t *TransactionManagerImpl) Begin() int64 {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.increaseXidCounter()
	t.updateXidStatus(t.xidCounter, ACTIVE)
	return t.xidCounter
}

func (t *TransactionManagerImpl) Commit(xid int64) {
	if xid == SuperXID {
		return
	}
	if xid > t.xidCounter {
		panic("Invalid Xid\n")
	}
	// update status
	t.updateXidStatus(xid, COMMITTED)
}

func (t *TransactionManagerImpl) Abort(xid int64) {
	if xid == SuperXID {
		return
	}
	if xid > t.xidCounter {
		panic("Invalid Xid\n")
	}
	t.updateXidStatus(xid, ABORTED)
}

func (t *TransactionManagerImpl) Status(xid int64) byte {
	if xid == SuperXID {
		return COMMITTED
	}
	if xid > t.xidCounter {
		panic("Invalid Xid\n")
	}
	offset := t.getXidOffset(xid)
	buf := make([]byte, XidStatusSize)
	if _, err := t.file.ReadAt(buf, offset); err != nil {
		panic(err)
	}
	return buf[0]
}

func (t *TransactionManagerImpl) close() {
	_ = t.file.Close()
}

func (t *TransactionManagerImpl) initXidFile() {
	bytesBuffer := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(bytesBuffer, binary.BigEndian, int64(0)); err != nil {
		panic(err)
	}
	if _, err := t.file.Write(bytesBuffer.Bytes()); err != nil {
		panic(err)
	}
}

// 获得xid事物状态信息在xid文件中的偏移量
func (t *TransactionManagerImpl) getXidOffset(xid int64) int64 {
	return (xid-1)*XidStatusSize + XidHeaderLength
}

// 更新事物的状态
func (t *TransactionManagerImpl) updateXidStatus(xid int64, status byte) {
	offset := t.getXidOffset(xid)
	buf := make([]byte, XidStatusSize)
	buf[0] = status
	if _, err := t.file.WriteAt(buf, offset); err != nil {
		panic(err)
	}
}

func (t *TransactionManagerImpl) increaseXidCounter() {
	t.xidCounter++
	// update length header
	bytesBuffer := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(bytesBuffer, binary.BigEndian, t.xidCounter); err != nil {
		panic(err)
	}
	if _, err := t.file.WriteAt(bytesBuffer.Bytes(), 0); err != nil {
		panic(err)
	}
}

/*//  Debug only for unit test
func (t *TransactionManagerImpl) Debug() os.FileInfo {
	stat, _ := t.file.Stat()
	return stat
}
*/
