package network

//import (
//	"errors"
//	"fmt"
//	cmap "github.com/orcaman/concurrent-map/v2"
//	"myDB/server/iface"
//	"strconv"
//)
//
//type cid struct {
//	id uint32
//}
//
//func (c cid) String() string {
//	return strconv.FormatInt(int64(c.id), 10)
//}
//
//type ConnectionManager struct {
//	ConnectionMap cmap.ConcurrentMap[cid, iface.IConnection]
//}
//
//func NewConnectionManager() iface.IConnectionManager {
//	return &ConnectionManager{
//		ConnectionMap: cmap.NewStringer[cid, iface.IConnection](),
//	}
//}
//
//func (cm *ConnectionManager) Add(c iface.IConnection) {
//	cm.ConnectionMap.Set(cid{id: c.GetConnId()}, c)
//	fmt.Printf("[ConnectionManager Add Connection] Add Connection %d success\n", c.GetConnId())
//}
//
//func (cm *ConnectionManager) Remove(c iface.IConnection) {
//	cm.ConnectionMap.Remove(cid{id: c.GetConnId()})
//	fmt.Printf("[ConnectionManager Remove Connection] Remove Connection %d success\n", c.GetConnId())
//}
//
//func (cm *ConnectionManager) Get(c uint32) (iface.IConnection, error) {
//	if cm.ConnectionMap.Has(cid{id: c}) {
//		c, _ := cm.ConnectionMap.Get(cid{id: c})
//		return c, nil
//	}
//	return nil, errors.New(fmt.Sprintf("[ConnectionManager Get Connection ERROR] Invalid Connection id %d\n", c))
//}
//
//func (cm *ConnectionManager) Total() int {
//	return cm.ConnectionMap.Count()
//}
//
//func (cm *ConnectionManager) ClearAll() {
//	for _, k := range cm.ConnectionMap.Keys() {
//		conn, _ := cm.ConnectionMap.Get(k)
//		conn.Stop()
//	}
//	for _, k := range cm.ConnectionMap.Keys() {
//		cm.ConnectionMap.Remove(k)
//	}
//	fmt.Printf("[Connection Manager Remove All] All connections removed.\n")
//}
