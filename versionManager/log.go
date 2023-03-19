package versionManager

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
)

// UNDO LOG

const (
	UndoSuffix string = "_undo.log"
	SzUndoData int64  = 8
)

// data format of undo log [length]8[Record Raw]

type Log interface {
	Read(offset int64) []byte
	Log(data []byte) int64
}

type UndoLog struct {
	file   *os.File
	offset int64
	lock   *sync.Mutex
}

func (undo *UndoLog) Read(offset int64) []byte {
	// unlocked
	buffer := make([]byte, SzUndoData)
	if _, err := undo.file.ReadAt(buffer, offset); err != nil {
		panic(fmt.Sprintf("Error occurs when reading data, err = %s", err))
	}
	length := int64(binary.BigEndian.Uint64(buffer))
	raw := make([]byte, length)
	if _, err := undo.file.ReadAt(raw, offset+SzUndoData); err != nil {
		panic(fmt.Sprintf("Error occurs when reading data, err = %s", err))
	}
	return raw
}

// Log
// append a log in undo log and return the offset of the log
// data -> log data
func (undo *UndoLog) Log(data []byte) int64 {
	undo.lock.Lock()
	defer undo.lock.Unlock()
	ret := undo.offset
	raw := wrapUndoLog(data)
	if n, err := undo.file.Write(raw); err != nil {
		panic(fmt.Sprintf("Error occurs when logging undo log, err = %s"))
	} else {
		undo.offset += int64(n)
	}
	return ret
}

func OpenUndoLog(path string, lock *sync.Mutex) Log {
	f, err := os.OpenFile(path+UndoSuffix, os.O_RDWR, 0666)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return createUndoLog(path, lock)
	} else if err != nil {
		panic(fmt.Sprintf("Error occurs when opening undo log, err = %s", err))
	}
	stat, _ := f.Stat()
	undo := &UndoLog{
		file:   f,
		lock:   lock,
		offset: stat.Size(),
	}
	return undo
}

func wrapUndoLog(data []byte) []byte {
	length := len(data)
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.BigEndian, int64(length))
	return append(buffer.Bytes(), data...)
}

func createUndoLog(path string, lock *sync.Mutex) Log {
	f, err := os.Create(path + UndoSuffix)
	if err != nil {
		panic(fmt.Sprintf("Error occurs when creating undo log, err = %s", err))
	}
	if err := f.Truncate(1); err != nil {
		panic(fmt.Sprintf("Error occurs when creating undo log, err = %s", err))
	}
	undo := &UndoLog{
		file:   f,
		lock:   lock,
		offset: 1,
	}
	return undo
}
