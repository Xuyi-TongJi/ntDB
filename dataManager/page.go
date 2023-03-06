package dataManager

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"reflect"
	"sync"
)

// 页及页缓存及其实现

const (
	PageSize int64 = 8192 // 8K bytes
)

type Page interface {
	PoolObj
	Lock()
	Unlock()
	Insert(toAdd []byte) error                  // 插入数据
	RecoverData(raw []byte, offset int64) error // 异常恢复数据
	Update(toUp []byte, offset int64) error     // 更新数据
}

type PageType int32

const (
	DbMetaPage    PageType = 1<<0 | 1<<15
	TableMetaPage PageType = 1<<0 | 1<<16
	IndexPage     PageType = 1<<1 | 1<<17
	RecordPage    PageType = 1<<1 | 1<<18

	VcOn     = 100
	VcOffset = 8
	VcOff    = VcOn + VcOffset
)

type PageImpl struct {
	lock     sync.Mutex
	data     []byte
	dirty    bool
	pageId   int64
	pc       PageCache // 每个Page组合一个PageCache，可以在操作页面时对页面缓存进行操作
	pageType PageType  // page的种类 元数据/普通页/索引页
}

func (p *PageImpl) Lock() {
	p.lock.Lock()
}

func (p *PageImpl) Unlock() {
	p.lock.Unlock()
}

func (p *PageImpl) IsDirty() bool {
	return p.dirty
}

func (p *PageImpl) SetDirty(dirty bool) {
	p.dirty = dirty
}

func (p *PageImpl) GetId() int64 {
	return p.pageId
}

func (p *PageImpl) GetData() []byte {
	return p.data
}

func (p *PageImpl) GetOffset() int64 {
	return (p.pageId - 1) * PageSize
}

func (p *PageImpl) GetDataSize() int64 {
	return PageSize
}

func (p *PageImpl) SetData(data []byte) {
	p.data = data
}

// 数据库元数据页管理

// CheckInitVersion
// 启动检查，检查进程上次退出是否是意外退出
// 如果是意外退出，则上层需要执行恢复数据的逻辑
func (p *PageImpl) CheckInitVersion() bool {
	if p.pageType != DbMetaPage {
		panic("Invalid page type when executing version checking\n")
	}
	data := p.GetData()
	v1, v2 := data[VcOn:VcOn+VcOffset], data[VcOff:VcOff+VcOffset]
	return reflect.DeepEqual(v1, v2)
}

// InitVersion 初始化版本号, 仅当系统启动时调用
func (p *PageImpl) InitVersion() {
	if p.pageType != DbMetaPage {
		panic("Invalid page type when executing version checking\n")
	}
	data := p.GetData()
	if _, err := rand.Read(data[VcOn : VcOn+VcOffset]); err != nil {
		panic("Error happen when initializing version\n")
	}
}

// UpdateVersion 更新包版本号, 仅当系统正常退出时调用
func (p *PageImpl) UpdateVersion() {
	if p.pageType != DbMetaPage {
		panic("Invalid page type when executing version checking\n")
	}
	data := p.GetData()
	copy(data[VcOff:VcOff+VcOffset], data[VcOn:VcOn+VcOffset])
}

// 普通页管理
// 首2个字节代表本页空闲位置偏移, 其余所有空间用于存储数据

type ErrorPageOverFlow struct{}

func (e *ErrorPageOverFlow) Error() string {
	return "Page Space overflow"
}

// Insert 向页末尾添加数据
func (p *PageImpl) Insert(toAdd []byte) error {
	used, length := p.getUsed(), int64(len(toAdd))
	if length+used > PageSize {
		return &ErrorPageOverFlow{}
	}
	p.Lock()
	defer p.Unlock()
	copy(p.GetData()[used:used+length], toAdd)
	p.setUsed(uint16(used + length))
	p.SetDirty(true)
	return nil
}

func (p *PageImpl) RecoverData(raw []byte, offset int64) error {
	return p.Update(raw, offset)
}

func (p *PageImpl) Update(toUp []byte, offset int64) error {
	length := int64(len(toUp))
	if length+offset > PageSize {
		return &ErrorPageOverFlow{}
	}
	p.Lock()
	defer p.Lock()
	copy(p.GetData()[offset:offset+length], toUp)
	currentLength := p.getUsed()
	if length+offset > currentLength {
		p.setUsed(uint16(length + offset))
	}
	p.SetDirty(true)
	return nil
}

func (p *PageImpl) getUsed() int64 {
	buf := p.GetData()[:2]
	return int64(binary.BigEndian.Uint16(buf))
}

func (p *PageImpl) setUsed(used uint16) {
	buf := bytes.NewBuffer([]byte{})
	_ = binary.Write(buf, binary.BigEndian, used)
	copy(p.GetData()[:2], buf.Bytes())
}

func (p *PageImpl) getFree() int64 {
	return PageSize - p.getUsed()
}
