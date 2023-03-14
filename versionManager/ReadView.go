package versionManager

// Read View MVCC 读视图
// 可重复读隔离级别 -> 仅在事物开始时创建一次ReadView
// 读已提交隔离级别 -> 每次进行快照读时创建ReadView

type ReadView struct {
	creatorId int64   // the creator XID of this read view
	maxXid    int64   // the next XID the system will create
	minXid    int64   // the minimal active XID currently in the system
	xidList   []int64 // active XID currently in the system
}

func NewReadView(vm VersionManager, creatorId int64) *ReadView {
	// TODO
	return nil
}
