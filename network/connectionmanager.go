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
	Transmogrifier                *TopologyTransmogrifier
	topology                      *configuration.Topology
	cellTail                      *cc.ChanCellTail
	enqueueQueryInner             func(connectionManagerMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan                     <-chan connectionManagerMsg
	servers                       map[string]*connectionManagerMsgServerEstablished
	rmToServer                    map[common.RMId]*connectionManagerMsgServerEstablished
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

type connectionManagerMsgServerEstablished struct {
	*Connection
	send        func([]byte)
	established bool
	host        string
	rmId        common.RMId
	bootCount   uint32
	tieBreak    uint32
	rootId      *common.VarUUId
}

func (cmmse *connectionManagerMsgServerEstablished) connectionManagerMsgWitness() {}

type connectionManagerMsgServerLost struct {
	*Connection
	rmId common.RMId
}

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

func (cm *ConnectionManager) ServerEstablished(conn *Connection, host string, rmId common.RMId, bootCount uint32, tieBreak uint32, rootId *common.VarUUId) {
	cm.enqueueQuery(&connectionManagerMsgServerEstablished{
		Connection:  conn,
		send:        conn.Send,
		established: true,
		host:        host,
		rmId:        rmId,
		bootCount:   bootCount,
		tieBreak:    tieBreak,
		rootId:      rootId,
	})
}

func (cm *ConnectionManager) ServerLost(conn *Connection, rmId common.RMId) {
	cm.enqueueQuery(&connectionManagerMsgServerLost{
		Connection: conn,
		rmId:       rmId,
	})
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

func NewConnectionManager(rmId common.RMId, bootCount uint32, procs int, disk *mdbs.MDBServer, nodeCertPrivKeyPair *certs.NodeCertificatePrivateKeyPair, port uint16, config *configuration.Configuration) (*ConnectionManager, *TopologyTransmogrifier) {
	cm := &ConnectionManager{
		RMId:                          rmId,
		BootCount:                     bootCount,
		NodeCertificatePrivateKeyPair: nodeCertPrivKeyPair,
		servers:           make(map[string]*connectionManagerMsgServerEstablished),
		rmToServer:        make(map[common.RMId]*connectionManagerMsgServerEstablished),
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
	cd := &connectionManagerMsgServerEstablished{
		send:        cm.Send,
		established: true,
		rmId:        rmId,
		bootCount:   bootCount,
	}
	cm.rmToServer[cd.rmId] = cd
	cm.servers[cd.host] = cd
	lc := client.NewLocalConnection(rmId, bootCount, cm)
	cm.Dispatchers = paxos.NewDispatchers(cm, rmId, uint8(procs), disk, lc)
	transmogrifier, localEstablished := NewTopologyTransmogrifier(cm, lc, port, config)
	cm.Transmogrifier = transmogrifier
	go cm.actorLoop(head)
	cm.ClientEstablished(0, lc)
	<-localEstablished
	return cm, transmogrifier
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
				cm.serverEstablished(msgT)
			case *connectionManagerMsgServerLost:
				cm.serverLost(msgT)
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
	for _, cd := range cm.servers {
		cd.Shutdown(true)
	}
	if shutdownChan != nil {
		close(shutdownChan)
	}
}

func (cm *ConnectionManager) setDesiredServers(hosts *connectionManagerMsgSetDesired) {
	oldServers := cm.servers
	delete(oldServers, cm.localHost)
	localHostChanged := cm.localHost != hosts.local
	cm.Lock()
	cm.localHost = hosts.local
	cm.Unlock()
	cm.desired = hosts.remote
	cm.servers = make(map[string]*connectionManagerMsgServerEstablished)
	cd := cm.rmToServer[cm.RMId]
	if localHostChanged {
		cm.sendersConnectionLost(cd.rmId)
		cd = cd.clone()
		cd.host = cm.localHost
		cm.rmToServer[cd.rmId] = cd
		cm.servers[cm.localHost] = cd
		cm.sendersConnectionEstablished(cd.rmId, cd)
	}

	for _, host := range hosts.remote {
		if cd, found := oldServers[host]; found {
			cm.servers[host] = cd
			delete(oldServers, host)
		} else {
			cm.servers[host] = &connectionManagerMsgServerEstablished{
				Connection: NewConnectionToDial(host, cm),
				host:       host,
			}
		}
	}

	for _, cd := range oldServers {
		cd.Shutdown(false)
		if cd.established {
			delete(cm.rmToServer, cd.rmId)
			cm.sendersConnectionLost(cd.rmId)
		}
	}
}

func (cm *ConnectionManager) serverEstablished(connEst *connectionManagerMsgServerEstablished) {
	if cd, found := cm.servers[connEst.host]; found && cd.Connection == connEst.Connection {
		// fall through to where we do the safe insert of connEst

	} else if found {
		killOld, killNew := false, false
		switch {
		case cd.established && cd.rmId != connEst.rmId:
			killOld, killNew = true, true
		case !cd.established || cd.bootCount < connEst.bootCount:
			killOld = true
		case cd.bootCount > connEst.bootCount:
			killNew = true
		case cd.tieBreak == connEst.tieBreak:
			killOld, killNew = true, true
		default:
			killOld = cd.tieBreak < connEst.tieBreak
			killNew = !killOld
		}

		if killOld {
			cd.Shutdown(false)
			if cd.established {
				delete(cm.rmToServer, cd.rmId)
				cm.sendersConnectionLost(cd.rmId)
			}
		}
		if killNew {
			connEst.Shutdown(false)
			if killOld {
				cm.servers[cd.host] = &connectionManagerMsgServerEstablished{
					Connection: NewConnectionToDial(cd.host, cm),
					host:       cd.host,
				}
			}
			return
		}

	} else {
		cm.servers[connEst.host] = connEst
	}

	if cd, found := cm.rmToServer[connEst.rmId]; found && connEst.rmId == cm.RMId {
		log.Printf("%v is claiming to have the same RMId as ourself! (%v)",
			connEst.host, cm.RMId)
		connEst.Shutdown(false)
		cm.servers[connEst.host] = &connectionManagerMsgServerEstablished{
			Connection: NewConnectionToDial(connEst.host, cm),
			host:       connEst.host,
		}

	} else if found {
		log.Printf("%v claimed by multiple servers: %v and %v. Recreating both connections.",
			connEst.rmId, cd.host, connEst.host)
		cd.Shutdown(false)
		connEst.Shutdown(false)
		delete(cm.rmToServer, cd.rmId)
		cm.sendersConnectionLost(cd.rmId)
		cm.servers[cd.host] = &connectionManagerMsgServerEstablished{
			Connection: NewConnectionToDial(cd.host, cm),
			host:       cd.host,
		}
		cm.servers[connEst.host] = &connectionManagerMsgServerEstablished{
			Connection: NewConnectionToDial(connEst.host, cm),
			host:       connEst.host,
		}

	} else {
		cm.servers[connEst.host] = connEst
		cm.rmToServer[connEst.rmId] = connEst
		cm.sendersConnectionEstablished(connEst.rmId, connEst)
	}
}

func (cm *ConnectionManager) serverLost(connLost *connectionManagerMsgServerLost) {
	rmId := connLost.rmId
	if cd, found := cm.rmToServer[connLost.rmId]; found && cd.Connection == connLost.Connection {
		log.Printf("Connection to RMId %v lost\n", rmId)
		cd.established = false
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

func (cm *ConnectionManager) sendersConnectionEstablished(rmId common.RMId, cd *connectionManagerMsgServerEstablished) {
	rmToServerCopy := cm.cloneRMToServer()
	for sender := range cm.senders {
		sender.ConnectionEstablished(rmId, cd, rmToServerCopy)
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
	cd := cm.rmToServer[cm.RMId]
	if !topology.Root.VarUUId.Equal(cd.rootId) {
		cm.sendersConnectionLost(cd.rmId)
		cd = cd.clone()
		cd.rootId = topology.Root.VarUUId
		cm.rmToServer[cm.RMId] = cd
		cm.servers[cd.host] = cd
		cm.sendersConnectionEstablished(cd.rmId, cd)
	}
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
		if conn.Connection != nil {
			conn.Connection.Status(sc.Fork())
		}
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

func (cd *connectionManagerMsgServerEstablished) Host() string {
	return cd.host
}

func (cd *connectionManagerMsgServerEstablished) RMId() common.RMId {
	return cd.rmId
}

func (cd *connectionManagerMsgServerEstablished) BootCount() uint32 {
	return cd.bootCount
}

func (cd *connectionManagerMsgServerEstablished) TieBreak() uint32 {
	return cd.tieBreak
}

func (cd *connectionManagerMsgServerEstablished) RootId() *common.VarUUId {
	return cd.rootId
}

func (cd *connectionManagerMsgServerEstablished) Send(msg []byte) {
	cd.send(msg)
}

func (cd *connectionManagerMsgServerEstablished) Shutdown(sync bool) {
	if cd.Connection != nil {
		cd.Connection.Shutdown(sync)
	}
}

func (cd *connectionManagerMsgServerEstablished) clone() *connectionManagerMsgServerEstablished {
	return &connectionManagerMsgServerEstablished{
		Connection:  cd.Connection,
		send:        cd.send,
		established: cd.established,
		host:        cd.host,
		rmId:        cd.rmId,
		bootCount:   cd.bootCount,
		tieBreak:    cd.tieBreak,
		rootId:      cd.rootId,
	}
}
