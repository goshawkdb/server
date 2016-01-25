package network

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	cc "github.com/msackman/chancell"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/common/certs"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	"log"
	"sync"
)

type ConnectionManager struct {
	sync.RWMutex
	localHost                     string
	RMId                          common.RMId
	BootCount                     uint32
	NodeCertificatePrivateKeyPair *certs.NodeCertificatePrivateKeyPair
	topology                      *configuration.Topology
	cellTail                      *cc.ChanCellTail
	enqueueQueryInner             func(connectionManagerMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan                     <-chan connectionManagerMsg
	servers                       map[string]*Connection
	rmToServer                    map[common.RMId]*connectionDetails
	connCountToClient             map[uint32]paxos.ClientConnection
	desired                       []string
	senders                       map[paxos.Sender]server.EmptyStruct
	Dispatchers                   *paxos.Dispatchers
}

type connectionManagerMsg interface {
	connectionManagerMsgWitness()
}

type connectionManagerMsgShutdown chan struct{}

func (cmms connectionManagerMsgShutdown) connectionManagerMsgWitness() {}

type connectionManagerMsgSetDesired struct {
	local  string
	remote []string
}

func (cmmsd *connectionManagerMsgSetDesired) connectionManagerMsgWitness() {}

type connectionManagerMsgServerEstablished Connection

func (cmmse *connectionManagerMsgServerEstablished) connectionManagerMsgWitness() {}

type connectionManagerMsgServerLost Connection

func (cmmsl *connectionManagerMsgServerLost) connectionManagerMsgWitness() {}

type connectionManagerMsgClientEstablished struct {
	connNumber uint32
	conn       paxos.ClientConnection
	servers    map[common.RMId]paxos.Connection
	topology   *configuration.Topology
	resultChan chan struct{}
}

func (cmmce *connectionManagerMsgClientEstablished) connectionManagerMsgWitness() {}

type connectionManagerMsgSenderStart struct{ paxos.Sender }

func (cmms connectionManagerMsgSenderStart) connectionManagerMsgWitness() {}

type connectionManagerMsgSenderFinished struct {
	paxos.Sender
	resultChan chan struct{}
}

func (cmms connectionManagerMsgSenderFinished) connectionManagerMsgWitness() {}

type connectionManagerMsgGetTopology struct {
	resultChan chan struct{}
	topology   *configuration.Topology
}

func (cmmgt *connectionManagerMsgGetTopology) connectionManagerMsgWitness() {}

type connectionManagerMsgSetTopology configuration.Topology

func (cmmst *connectionManagerMsgSetTopology) connectionManagerMsgWitness() {}

type connectionManagerMsgStatus server.StatusConsumer

func (cmms *connectionManagerMsgStatus) connectionManagerMsgWitness() {}

func (cm *ConnectionManager) Shutdown() {
	c := make(chan struct{})
	cm.enqueueSyncQuery(connectionManagerMsgShutdown(c), c)
	<-c
}

func (cm *ConnectionManager) SetDesiredServers(localhost string, remotehosts []string) {
	cm.enqueueQuery(&connectionManagerMsgSetDesired{
		local:  localhost,
		remote: remotehosts,
	})
}

func (cm *ConnectionManager) ServerEstablished(conn *Connection) {
	cm.enqueueQuery((*connectionManagerMsgServerEstablished)(conn))
}

func (cm *ConnectionManager) ServerLost(conn *Connection) {
	cm.enqueueQuery((*connectionManagerMsgServerLost)(conn))
}

func (cm *ConnectionManager) ClientEstablished(connNumber uint32, conn paxos.ClientConnection) (*configuration.Topology, map[common.RMId]paxos.Connection) {
	query := &connectionManagerMsgClientEstablished{
		connNumber: connNumber,
		conn:       conn,
		resultChan: make(chan struct{}),
	}
	if cm.enqueueSyncQuery(query, query.resultChan) {
		return query.topology, query.servers
	} else {
		return nil, nil
	}
}

func (cm *ConnectionManager) ClientLost(connNumber uint32) {
	cm.Lock()
	delete(cm.connCountToClient, connNumber)
	cm.Unlock()
}

func (cm *ConnectionManager) GetClient(bootNumber, connNumber uint32) paxos.ClientConnection {
	if bootNumber != cm.BootCount && bootNumber != 0 {
		return nil
	}
	cm.RLock()
	defer cm.RUnlock()
	return cm.connCountToClient[connNumber]
}

func (cm *ConnectionManager) LocalHost() string {
	cm.RLock()
	defer cm.RUnlock()
	return cm.localHost
}

func (cm *ConnectionManager) Topology() *configuration.Topology {
	query := &connectionManagerMsgGetTopology{
		resultChan: make(chan struct{}),
	}
	if cm.enqueueSyncQuery(query, query.resultChan) {
		return query.topology
	}
	return nil
}

func (cm *ConnectionManager) SetTopology(topology *configuration.Topology) {
	cm.enqueueQuery((*connectionManagerMsgSetTopology)(topology))
}

func (cm *ConnectionManager) AddSender(sender paxos.Sender) {
	cm.enqueueQuery(connectionManagerMsgSenderStart{sender})
}

func (cm *ConnectionManager) RemoveSenderAsync(sender paxos.Sender) {
	cm.enqueueQuery(connectionManagerMsgSenderFinished{Sender: sender})
}

func (cm *ConnectionManager) RemoveSenderSync(sender paxos.Sender) {
	resultChan := make(chan struct{})
	cm.enqueueSyncQuery(connectionManagerMsgSenderFinished{Sender: sender, resultChan: resultChan}, resultChan)
}

func (cm *ConnectionManager) Status(sc *server.StatusConsumer) {
	cm.enqueueQuery((*connectionManagerMsgStatus)(sc))
}

func (cm *ConnectionManager) enqueueQuery(msg connectionManagerMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return cm.enqueueQueryInner(msg, cell, f)
	}
	return cm.cellTail.WithCell(f)
}

func (cm *ConnectionManager) enqueueSyncQuery(msg connectionManagerMsg, resultChan chan struct{}) bool {
	if cm.enqueueQuery(msg) {
		select {
		case <-resultChan:
			return true
		case <-cm.cellTail.Terminated:
			return false
		}
	} else {
		return false
	}
}

func NewConnectionManager(rmId common.RMId, bootCount uint32, procs int, disk *mdbs.MDBServer, nodeCertPrivKeyPair *certs.NodeCertificatePrivateKeyPair, clusterId string, port uint16) (*ConnectionManager, *TopologyTransmogrifier, error) {
	cm := &ConnectionManager{
		RMId:                          rmId,
		BootCount:                     bootCount,
		NodeCertificatePrivateKeyPair: nodeCertPrivKeyPair,
		servers:           make(map[string]*Connection),
		rmToServer:        make(map[common.RMId]*connectionDetails),
		connCountToClient: make(map[uint32]paxos.ClientConnection),
		desired:           nil,
		senders:           make(map[paxos.Sender]server.EmptyStruct),
	}
	var head *cc.ChanCellHead
	head, cm.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan connectionManagerMsg, n)
			cell.Open = func() { cm.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			cm.enqueueQueryInner = func(msg connectionManagerMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
				if curCell == cell {
					select {
					case queryChan <- msg:
						return true, nil
					default:
						return false, nil
					}
				} else {
					return false, cont
				}
			}
		})
	cm.rmToServer[rmId] = &connectionDetails{connectionSend: cm, bootCount: bootCount}
	lc := client.NewLocalConnection(rmId, bootCount, cm)
	cm.Dispatchers = paxos.NewDispatchers(cm, rmId, uint8(procs), disk, lc)
	localEstablished := NewTopologyTransmogrifier(cm, lc, clusterId, port)
	go cm.actorLoop(head)
	cm.ClientEstablished(0, lc)
	transmogrifier, err := localEstablished()
	return cm, transmogrifier, err
}

func (cm *ConnectionManager) actorLoop(head *cc.ChanCellHead) {
	var (
		err       error
		queryChan <-chan connectionManagerMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = cm.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	var shutdownChan chan struct{}
	for !terminate {
		if msg, ok := <-queryChan; ok {
			switch msgT := msg.(type) {
			case connectionManagerMsgShutdown:
				shutdownChan = msgT
				terminate = true
			case *connectionManagerMsgSetDesired:
				cm.setDesiredServers(msgT)
			case *connectionManagerMsgServerEstablished:
				cm.serverEstablished((*Connection)(msgT))
			case *connectionManagerMsgServerLost:
				cm.serverLost((*Connection)(msgT))
			case connectionManagerMsgSenderStart:
				cm.startSender(msgT.Sender)
			case connectionManagerMsgSenderFinished:
				cm.removeSender(msgT.Sender, msgT.resultChan)
			case *connectionManagerMsgSetTopology:
				cm.updateTopology((*configuration.Topology)(msgT))
			case *connectionManagerMsgGetTopology:
				cm.getTopology(msgT)
			case *connectionManagerMsgClientEstablished:
				cm.clientEstablished(msgT)
			case *connectionManagerMsgStatus:
				cm.status((*server.StatusConsumer)(msgT))
			}
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	if err != nil {
		log.Println("ConnectionManager error:", err)
	}
	cm.cellTail.Terminate()
	for _, conn := range cm.servers {
		conn.Shutdown(true)
	}
	if shutdownChan != nil {
		close(shutdownChan)
	}
}

func (cm *ConnectionManager) setDesiredServers(hosts *connectionManagerMsgSetDesired) {
	cm.Lock()
	cm.localHost = hosts.local
	cm.Unlock()
	cm.desired = hosts.remote
	oldServers := cm.servers
	cm.servers = make(map[string]*Connection)
	for _, host := range hosts.remote {
		if conn, found := oldServers[host]; found {
			cm.servers[host] = conn
			delete(oldServers, host)
		} else {
			cm.servers[host] = NewConnectionToDial(host, cm)
		}
	}

	for _, conn := range oldServers {
		_, _, rmId, _, _, _ := conn.RemoteDetails()
		if c, found := cm.rmToServer[rmId]; found && c.connectionSend == conn {
			delete(cm.rmToServer, rmId)
			cm.sendersConnectionLost(rmId)
		}
	}
	for _, conn := range oldServers {
		conn.Shutdown(false)
	}
}

func (cm *ConnectionManager) serverEstablished(conn *Connection) {
	established, host, rmId, rootId, bootCount, tiebreak := conn.RemoteDetails()
	if !established {
		return
	}
	if c, found := cm.servers[host]; found && c == conn {
		cwbc := &connectionDetails{connectionSend: conn, bootCount: bootCount, host: host, rootId: rootId}
		cm.rmToServer[rmId] = cwbc
		cm.sendersConnectionEstablished(rmId, cwbc)
		return
	} else if found {
		establishedC, _, _, _, bootCountC, tiebreakC := c.RemoteDetails()
		killOld, killNew := false, false
		switch {
		case !establishedC || bootCountC < bootCount:
			killOld = true
		case bootCountC > bootCount:
			killNew = true
		case tiebreakC == tiebreak:
			killOld, killNew = true, true
		default:
			killOld = tiebreakC < tiebreak
			killNew = !killOld
		}
		switch {
		case killOld && killNew:
			conn.Shutdown(false)
			c.Shutdown(false)
			cm.servers[host] = NewConnectionToDial(host, cm)
			if c2, found := cm.rmToServer[rmId]; found && c2.connectionSend == c {
				delete(cm.rmToServer, rmId)
				cm.sendersConnectionLost(rmId)
			}
		case killOld:
			c.Shutdown(false)
			cm.servers[host] = conn
			cwbc := &connectionDetails{connectionSend: conn, bootCount: bootCount, host: host, rootId: rootId}
			cm.rmToServer[rmId] = cwbc
			cm.sendersConnectionEstablished(rmId, cwbc)
		case killNew:
			conn.Shutdown(false)
		}
	} else {
		cm.servers[host] = conn
		cwbc := &connectionDetails{connectionSend: conn, bootCount: bootCount, host: host, rootId: rootId}
		cm.rmToServer[rmId] = cwbc
		cm.sendersConnectionEstablished(rmId, cwbc)
	}
}

func (cm *ConnectionManager) serverLost(conn *Connection) {
	_, _, rmId, _, _, _ := conn.RemoteDetails()
	if c, found := cm.rmToServer[rmId]; found && c.connectionSend == conn {
		log.Printf("Connection to RMId %v lost\n", rmId)
		delete(cm.rmToServer, rmId)
		cm.sendersConnectionLost(rmId)
	}
}

func (cm *ConnectionManager) clientEstablished(msg *connectionManagerMsgClientEstablished) {
	cm.Lock()
	cm.connCountToClient[msg.connNumber] = msg.conn
	cm.Unlock()
	msg.topology = cm.topology
	msg.servers = cm.cloneRMToServer()
	close(msg.resultChan)
}

func (cm *ConnectionManager) startSender(sender paxos.Sender) {
	if _, found := cm.senders[sender]; found {
		server.Log(sender, "CM found duplicate add sender")
		return
	} else {
		cm.senders[sender] = server.EmptyStructVal
		rmToServerCopy := cm.cloneRMToServer()
		sender.ConnectedRMs(rmToServerCopy)
	}
}

func (cm *ConnectionManager) removeSender(sender paxos.Sender, resultChan chan struct{}) {
	delete(cm.senders, sender)
	if resultChan != nil {
		close(resultChan)
	}
}

func (cm *ConnectionManager) sendersConnectionEstablished(rmId common.RMId, conn *connectionDetails) {
	rmToServerCopy := cm.cloneRMToServer()
	for sender := range cm.senders {
		sender.ConnectionEstablished(rmId, conn, rmToServerCopy)
	}
}

func (cm *ConnectionManager) sendersConnectionLost(rmId common.RMId) {
	rmToServerCopy := cm.cloneRMToServer()
	for sender := range cm.senders {
		sender.ConnectionLost(rmId, rmToServerCopy)
	}
}

func (cm *ConnectionManager) updateTopology(topology *configuration.Topology) {
	if cm.topology != nil && cm.topology.Configuration.Equal(topology.Configuration) {
		return
	}
	cm.topology = topology
	server.Log("Topology change:", topology)
	rmToServerCopy := cm.cloneRMToServer()
	for _, cconn := range cm.connCountToClient {
		cconn.TopologyChange(topology, rmToServerCopy)
	}
}

func (cm *ConnectionManager) getTopology(msg *connectionManagerMsgGetTopology) {
	msg.topology = cm.topology
	close(msg.resultChan)
}

func (cm *ConnectionManager) cloneRMToServer() map[common.RMId]paxos.Connection {
	rmToServerCopy := make(map[common.RMId]paxos.Connection, len(cm.rmToServer))
	for rmId, server := range cm.rmToServer {
		rmToServerCopy[rmId] = server
	}
	return rmToServerCopy
}

func (cm *ConnectionManager) status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("Address: %v", cm.localHost))
	sc.Emit(fmt.Sprintf("Boot Count: %v", cm.BootCount))
	sc.Emit(fmt.Sprintf("Current Topology: %v", cm.topology))
	serverConnections := make([]string, 0, len(cm.servers))
	for server := range cm.servers {
		serverConnections = append(serverConnections, server)
	}
	sc.Emit(fmt.Sprintf("Senders: %v", len(cm.senders)))
	rms := make([]common.RMId, 0, len(cm.rmToServer))
	for rmId := range cm.rmToServer {
		rms = append(rms, rmId)
	}
	sc.Emit(fmt.Sprintf("Active Server RMIds: %v", rms))
	sc.Emit(fmt.Sprintf("Active Server Connections: %v", serverConnections))
	sc.Emit(fmt.Sprintf("Desired Server Connections: %v", cm.desired))
	for _, conn := range cm.servers {
		conn.Status(sc.Fork())
	}
	sc.Emit(fmt.Sprintf("Client Connection Count: %v", len(cm.connCountToClient)))
	cm.connCountToClient[0].(*client.LocalConnection).Status(sc.Fork())
	for _, conn := range cm.connCountToClient {
		if c, ok := conn.(*Connection); ok {
			c.Status(sc.Fork())
		}
	}
	cm.Dispatchers.VarDispatcher.Status(sc.Fork())
	cm.Dispatchers.ProposerDispatcher.Status(sc.Fork())
	cm.Dispatchers.AcceptorDispatcher.Status(sc.Fork())
	sc.Join()
}

// paxos.Connection interface to allow sending to ourself.
func (cm *ConnectionManager) Send(b []byte) {
	seg, _, err := capn.ReadFromMemoryZeroCopy(b)
	server.CheckFatal(err)
	msg := msgs.ReadRootMessage(seg)
	cm.Dispatchers.DispatchMessage(cm.RMId, msg.Which(), &msg)
}

type connectionDetails struct {
	connectionSend
	bootCount uint32
	host      string
	rootId    *common.VarUUId
}

func (cd *connectionDetails) BootCount() uint32 {
	return cd.bootCount
}

func (cd *connectionDetails) Host() string {
	return cd.host
}

func (cd *connectionDetails) RootId() *common.VarUUId {
	return cd.rootId
}

type connectionSend interface {
	Send(msg []byte)
}
