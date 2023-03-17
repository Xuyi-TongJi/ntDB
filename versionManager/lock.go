package versionManager

// LockTable 记录当前VersionManager的锁状态
// 用于死锁检测

type LockTable interface {
}

type LockTableImpl struct{}
