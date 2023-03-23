package iface

/*
	封包，拆包模块
	面向TCP中的数据流，用于处理TCP粘包问题
*/

type IDataPack interface {

	// GetHeadLen 获取包的头长度方法
	GetHeadLen() uint32

	// Pack 封包
	Pack(msg IMessage) ([]byte, error)

	// Unpack 拆包
	Unpack([]byte) (IMessage, error)
}
