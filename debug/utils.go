package debug

import "log"

func UidTrans(uid int64) (pageId, offset int64) {
	offset = uid & ((1 << 32) - 1)
	uid >>= 32
	pageId = uid & ((1 << 32) - 1)
	log.Printf("[Data Manager] UID TRANS LOCATE AT %d %d\n", pageId, offset)
	return
}

func GetUid(pageId, offset int64) int64 {
	return (pageId << 32) | offset
}
