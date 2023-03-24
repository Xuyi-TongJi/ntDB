package utils

import (
	"encoding/json"
	"fmt"
	"myDB/server/iface"
	"myDB/versionManager"
	"os"
)

/*
	存储全局参数，供其他模块使用
	一些参数是可以由用户通过json配置
*/

type GlobalConfig struct {
	TcpServer        iface.IServer
	Name             string                        `json:"name"`
	Host             string                        `json:"host"`
	TcpPort          int                           `json:"tcpPort"`
	Version          string                        `json:"version"`
	MaxConn          int                           `json:"maxConn"`        // 最大连接数
	MaxPackingSize   uint32                        `json:"maxPackingSize"` // 当前服务器一次数据包的最大值
	WorkerPoolSize   uint32                        `json:"workerPoolSize"` // 当前业务工作Worker池的Goroutine数量
	BufferPoolMemory int64                         `json:"bufferPoolMemory"`
	Path             string                        `json:"path"` // 数据库文件路径
	Iso              versionManager.IsolationLevel // 数据库隔离级别
}

const (
	MaxWorkerPoolSize uint32 = 32
	DefaultFilePath   string = "./test/test"
)

var GlobalObj *GlobalConfig

// init 初始化对象
func init() {
	// 默认配置
	GlobalObj = &GlobalConfig{
		Name:             "default_server",
		Version:          "1.0",
		TcpPort:          3306,
		Host:             "0.0.0.0",
		MaxConn:          10,
		MaxPackingSize:   4096,
		WorkerPoolSize:   10,
		BufferPoolMemory: 1 << 20,
		Path:             DefaultFilePath,
		Iso:              1, // Default RR
	}
	// read json config
	GlobalObj.loadFormJson()
	if GlobalObj.WorkerPoolSize > MaxWorkerPoolSize {
		fmt.Printf("[Server Config WARNING] Server worker pool size is larger than max worker pool size,"+
			" size is reset to %d\n", MaxWorkerPoolSize)
		GlobalObj.WorkerPoolSize = MaxWorkerPoolSize
	}
}

func (g *GlobalConfig) loadFormJson() {
	data, err := os.ReadFile("../config/server_config.json")
	if err != nil {
		fmt.Printf("[Server Reading Config ERROR] Reading config error:%s\n", err)
	}
	err = json.Unmarshal(data, g)
	if err != nil {
		fmt.Printf("[Server Reading Config ERROR] Reading config error:%s\n", err)
	}
}
