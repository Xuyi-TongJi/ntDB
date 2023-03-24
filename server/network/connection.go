package network

import (
	"errors"
	"fmt"
	"log"
	util "myDB/dataStructure"
	"myDB/server/iface"
	"myDB/server/utils"
	"net"
	"strconv"
	"strings"

	"sync"
)

type Connection struct {
	TcpServer    iface.IServer
	Conn         *net.TCPConn
	ConnId       uint32
	IsClosed     bool
	ExitChan     chan bool
	MessageChan  chan []byte
	MsgHandler   iface.IMessageHandler
	PropertyMap  map[string]interface{}
	PropertyLock sync.RWMutex

	// request
	queryBuffer            []byte
	queryLength            int
	args                   []string
	bulkNum                int
	bulkLength             int
	isQueryProcessing      bool
	canDoNextCommandHandle bool
	reply                  util.LinkedList
	sentLength             int // 还未发送完的reply
}

// startReader 从当前连接读数据的模块
func (c *Connection) startReader() {
	fmt.Printf("[Connection Reader Goroutine] Connection %d reader gouroutine is running. Romote addr = %s\n",
		c.ConnId, c.GetClientTcpStatus().String())
	defer func() {
		fmt.Printf("[Connection Reader Goroutine] %s Connection %d was closed, reader goroutine closed\n",
			c.GetClientTcpStatus().String(), c.ConnId)
		c.Stop()
	}()
	for {
		maxQueryLength := int(c.TcpServer.GetMaxPackingSize())
		c.expandQueryBufIfNeeded()
		// Read
		n, err := c.Conn.Read(c.queryBuffer[c.queryLength:])
		if err != nil {
			log.Printf("[READ QUERY FROM CLIENT ERROR] Read query from client %d error, err = %s\n", c.ConnId, err)
			c.Stop()
			return
		}
		c.queryLength += n
		if c.queryLength > maxQueryLength {
			log.Printf("[READ QUERY FROM CLIENT ERROR] Client %d query length overflow error\n", c.ConnId)
			c.Stop()
			return
		}
		if err := c.processRequest(); err != nil {
			c.Stop()
		}
		if c.canDoNextCommandHandle {
			req := Request{
				conn:    c,
				message: &Message{Id: DbRouterMsgId, args: c.args},
			}
			c.canDoNextCommandHandle = false
			c.isQueryProcessing = true
			c.args = make([]string, 0)
			if utils.GlobalObj.WorkerPoolSize > 0 {
				// 有工作池池对象，将请求交给Message Handler 执行具体的业务逻辑
				c.MsgHandler.SubmitTask(&req)
			} else {
				go c.MsgHandler.DoHandle(&req)
			}
		}
	}
}

// startWriter 向当前连接写数据的模块
func (c *Connection) startWriter() {
	fmt.Printf("[Connection Writer Goroutine] Connection %d writer gouroutine is running. Romote addr = %s\n",
		c.ConnId, c.GetClientTcpStatus().String())
	defer fmt.Printf("[Connection Writer Goroutine] %s Connection %d was closed, writer goroutine closed\n",
		c.GetClientTcpStatus().String(), c.GetConnId())
	for {
		select {
		case data := <-c.MessageChan:
			if _, err := c.GetTcpConnection().Write(data); err != nil {
				fmt.Printf("[Connection Writer Goroutine ERROR] Connection %d writing back error: %s\n", c.ConnId, err)
				return
			} else {
				fmt.Printf("[Connection Writer Goroutine] Connection %d writing back to the client success\n",
					c.ConnId)
			}
		case <-c.ExitChan:
			// Reader已经退出
			return
		}
	}
}

// Start 启动连接，业务逻辑是启动一个读数据业务和一个写数据的业务
func (c *Connection) Start() {
	fmt.Printf("[Connection START] Connection %d starting\n", c.ConnId)
	c.TcpServer.CallOnConnectionStart(c)
	go c.startReader()
	go c.startWriter()
}

func (c *Connection) Stop() {
	if c.IsClosed {
		return
	}
	c.TcpServer.CallOnConnectionStop(c)
	c.ExitChan <- true
	c.TcpServer.GetConnectionManager().Remove(c)
	c.IsClosed = true
	err := c.Conn.Close()
	if err != nil {
		fmt.Printf("[Connection STOP ERROR] Connection %d stopped error:%s\n", c.ConnId, err)
	}
	fmt.Printf("[Connection STOP] Connection %d stopped success\n", c.ConnId)
	close(c.MessageChan)
	close(c.ExitChan)
}

func (c *Connection) GetTcpConnection() *net.TCPConn {
	return c.Conn
}

func (c *Connection) GetConnId() uint32 {
	return c.ConnId
}

func (c *Connection) GetClientTcpStatus() net.Addr {
	return c.GetTcpConnection().RemoteAddr()
}

// SendMessage 将数据封包为二进制数据并发送给写协程
func (c *Connection) SendMessage(msgId uint32, data []byte) error {
	if c.IsClosed {
		return errors.New(fmt.Sprintf("[Connection Writing GoRoutine] Connection %d was closed\n", c.ConnId))
	}
	dp := DataPack{}
	msg := &Message{
		Id: msgId,
		// TODO
		//Len:  uint32(len(data)),
		//Data: data,
	}
	// pack (message to binary data)
	binaryData, err := dp.Pack(msg)
	if err != nil {
		return errors.New(fmt.Sprintf("[Connection Writing GoRoutine] Connection %d, packing message error: %s\n", c.ConnId, err))
	}
	// 将data发送给写协程
	c.MessageChan <- binaryData
	return nil
}

func (c *Connection) SetConnectionProperty(key string, value interface{}) {
	c.PropertyLock.Lock()
	defer c.PropertyLock.Unlock()
	c.PropertyMap[key] = value
}

func (c *Connection) GetConnectionProperty(key string) interface{} {
	c.PropertyLock.RLock()
	defer c.PropertyLock.RUnlock()
	if value, ok := c.PropertyMap[key]; ok {
		return value
	} else {
		return nil
	}
}

func (c *Connection) RemoveConnectionProperty(key string) {
	c.PropertyLock.Lock()
	defer c.PropertyLock.Unlock()
	if _, ok := c.PropertyMap[key]; ok {
		delete(c.PropertyMap, key)
	}
}

func (c *Connection) HasClosed() bool {
	return c.IsClosed
}

// NewConnection 初始化连接模块的方法
func NewConnection(server iface.IServer, conn *net.TCPConn, id uint32, msgHandler iface.IMessageHandler) *Connection {
	c := &Connection{
		TcpServer:   server,
		Conn:        conn,
		ConnId:      id,
		IsClosed:    false,
		MsgHandler:  msgHandler,
		MessageChan: make(chan []byte),
		ExitChan:    make(chan bool, 1),
		PropertyMap: make(map[string]interface{}),
	}
	c.TcpServer.GetConnectionManager().Add(c)
	return c
}

// processRequest 处理请求 功能：将请求string转为Client对象中的args
// 1. 获取请求协议类型[INLINE/BULK]
// 2. 将请求[]byte解析道client.args
// TODO 解析发生错误，则断开连接
// 未完整解析一条指令，则保留queryBuffer和queryLength，到下一次Read(readQueryFromClient)返回后再处理
// 处理一定是从queryBuffer的第一个字节开始
func (c *Connection) processRequest() error {
	// 只要缓冲区还有未处理的queryBuffer就进行处理
	for c.queryLength > 0 {
		// 没有处理到一半的请求
		// query -> args
		if err := c.handleBulkRequest(); err != nil {
			return err
		}
		// 不能进行下一次processCommand(没有完整解析，即完整Read完整这一条指令),则break，等待下一次Read
		if !c.canDoNextCommandHandle {
			break
		}
	}
	return nil
}

// handleBulkRequest 解析Bulk请求string
// query string -> client.args
// 滑动窗口
// error -> 解析发生错误，则返回error，断开连接
func (c *Connection) handleBulkRequest() error {
	// new request -> bulkNum == 0
	if c.bulkNum == 0 {
		crlfIndex := c.findCrlfFromQueryBuffer()
		if crlfIndex == -1 {
			return errors.New(fmt.Sprintf("Query Length overflows\n"))
		}
		bNum, err := c.getNumberFromQueryBuffer(1, crlfIndex)
		if err != nil {
			return errors.New("illegal client protocol format, illegal bulk number")
		}
		c.isQueryProcessing = true
		c.canDoNextCommandHandle = false
		c.bulkNum = bNum
		// move sliding window
		c.queryBuffer = append(c.queryBuffer[crlfIndex+2:])
		c.queryLength -= crlfIndex + 2
	}
	for c.bulkNum > 0 {
		if len(c.queryBuffer) == 0 {
			break
		}
		// find bulkLength
		if c.bulkLength == 0 {
			if c.queryBuffer[0] != '$' {
				return errors.New("illegal client protocol format, illegal bulk length symbol")
			}
			crlfIndex := c.findCrlfFromQueryBuffer()
			if crlfIndex == -1 {
				break
			}
			bLength, err := c.getNumberFromQueryBuffer(1, crlfIndex)
			if err != nil {
				return errors.New("illegal client protocol format, illegal bulk length")
			}
			c.bulkLength = bLength
			// move sliding window
			c.queryBuffer = append(c.queryBuffer[crlfIndex+2:])
			c.queryLength -= crlfIndex + 2
		}
		// find next string element (based on bulkLength)
		if c.queryLength < c.bulkLength+2 {
			break
		}
		// build client arg
		newArg := string(c.queryBuffer[:c.bulkLength])
		c.args = append(c.args, newArg)
		c.queryBuffer = append(c.queryBuffer[c.bulkLength+2:])
		c.queryLength -= c.bulkLength + 2
		c.bulkLength = 0
		c.bulkNum -= 1
	}
	// 下一次command可以执行
	if c.bulkNum == 0 {
		c.isQueryProcessing = false
		c.canDoNextCommandHandle = true
	}
	return nil
}

// findCrlfFromQueryBuffer
// CRLF: \r\n
func (c *Connection) findCrlfFromQueryBuffer() int {
	return strings.Index(string(c.queryBuffer[:c.queryLength]), "\r\n")
}

func (c *Connection) getNumberFromQueryBuffer(startIndex, endIndex int) (int, error) {
	return strconv.Atoi(string(c.queryBuffer[startIndex:endIndex]))
}

func (c *Connection) expandQueryBufIfNeeded() {
	if len(c.queryBuffer)-c.queryLength < int(c.TcpServer.GetMaxPackingSize()) {
		c.queryBuffer = append(c.queryBuffer, make([]byte, int(c.TcpServer.GetMaxPackingSize()))...)
	}
}
