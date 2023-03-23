package network

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"myDB/server/iface"
	"myDB/server/utils"
)

// TLV协议解析器

type DataPack struct{}

func (d *DataPack) GetHeadLen() uint32 {
	// data len uint32 4bytes
	// data id uint32 4bytes
	return 8
}

// Pack 将封装好的Message以TLV格式输出为字节流
// 使用Buffer读
// 最终的binary data格式 len|id|data
func (d *DataPack) Pack(msg iface.IMessage) ([]byte, error) {
	buffer := bytes.NewBuffer([]byte{})
	// write data len 4bytes to buffer
	err := binary.Write(buffer, binary.LittleEndian, msg.GetLen())
	if err != nil {
		return nil, err
	}
	// write id 4bytes to buffer
	err = binary.Write(buffer, binary.LittleEndian, msg.GetMsgId())
	if err != nil {
		return nil, err
	}
	// write data to buffer
	err = binary.Write(buffer, binary.LittleEndian, msg.GetData())
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Unpack 读入并封装Message Head (len + id)
func (d *DataPack) Unpack(head []byte) (iface.IMessage, error) {
	msg := &Message{}
	// read head data by reader
	reader := bytes.NewReader(head)
	/* 最后一个参数是数据类型， &msg.Len uint32 代表读4个字节 并且读入到msg.Len赋值 */
	if err := binary.Read(reader, binary.LittleEndian, &msg.Len); err != nil {
		return nil, err
	}
	// dataLen > MaxPackaging size
	if msg.Len > utils.GlobalObj.MaxPackingSize {
		err := errors.New(fmt.Sprintf("[UnPackaging Message Refused] Data is larger than max size, length = %d", msg.Len))
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &msg.Id); err != nil {
		return nil, err
	}
	return msg, nil
}
