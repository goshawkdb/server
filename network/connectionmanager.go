package network

import (
	"encoding/binary"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	cc "github.com/msackman/chancell"
	"goshawkdb.io/common"
	"goshawkdb.io/common/certs"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"log"
	"sync"
)

type ShutdownSignaller interface {
	SignalShutdown()
}

type ConnectionManager struct {
	sync.RWMutex
	localHost                     string
	RMId                          common.RMId
	bootcount                     uint32
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
	serverConnSubscribers         serverConnSubscribers
	topologySubscribers           topologySubscribers
	Dispatchers                   *paxos.Dispatchers
}

type serverConnSubscribers struct {
	*ConnectionManager
	subscribers map[paxos.ServerConnectionSubscriber]server.EmptyStruct
}

type topologySubscribers struct {
	*ConnectionManager
	subscribers []map[eng.TopologySubscriber]server.EmptyStruct
}

func (cm *ConnectionManager) BootCount() uint32 {
	return cm.bootcount
}

func (cm *ConnectionManager) DispatchMessage(sender common.RMId, msgType msgs.Message_Which, msg msgs.Message) {
	d := cm.Dispatchers
	switch msgType {
	case msgs.MESSAGE_TXNSUBMISSION:
		txn := eng.TxnReaderFromData(msg.TxnSubmission())
		d.ProposerDispatcher.TxnReceived(sender, txn)
	case msgs.MESSAGE_SUBMISSIONOUTCOME:
		outcome := msg.SubmissionOutcome()
		txn := eng.TxnReaderFromData(outcome.Txn())
		txnId := txn.Id
		connNumber := binary.BigEndian.Uint32(txnId[8:12])
		bootNumber := binary.BigEndian.Uint32(txnId[12:16])
		if conn := cm.GetClient(bootNumber, connNumber); conn == nil {
			// OSS is safe here - it's the default action on receipt of outcome for unknown client.
			paxos.NewOneShotSender(paxos.MakeTxnSubmissionCompleteMsg(txnId), cm, sender)
		} else {
			conn.SubmissionOutcomeReceived(sender, txn, &outcome)
			return
		}
	case msgs.MESSAGE_SUBMISSIONCOMPLETE:
		tsc := msg.SubmissionComplete()
		d.AcceptorDispatcher.TxnSubmissionCompleteReceived(sender, &tsc)
	case msgs.MESSAGE_SUBMISSIONABORT:
		tsa := msg.SubmissionAbort()
		d.ProposerDispatcher.TxnSubmissionAbortReceived(sender, &tsa)
	case msgs.MESSAGE_ONEATXNVOTES:
		oneATxnVotes := msg.OneATxnVotes()
		d.AcceptorDispatcher.OneATxnVotesReceived(sender, &oneATxnVotes)
	case msgs.MESSAGE_ONEBTXNVOTES:
		oneBTxnVotes := msg.OneBTxnVotes()
		d.ProposerDispatcher.OneBTxnVotesReceived(sender, &oneBTxnVotes)
	case msgs.MESSAGE_TWOATXNVOTES:
		twoATxnVotes := msg.TwoATxnVotes()
		d.AcceptorDispatcher.TwoATxnVotesReceived(sender, &twoATxnVotes)
	case msgs.MESSAGE_TWOBTXNVOTES:
		twoBTxnVotes := msg.TwoBTxnVotes()
		d.ProposerDispatcher.TwoBTxnVotesReceived(sender, &twoBTxnVotes)
	case msgs.MESSAGE_TXNLOCALLYCOMPLETE:
		tlc := msg.TxnLocallyComplete()
		d.AcceptorDispatcher.TxnLocallyCompleteReceived(sender, &tlc)
	case msgs.MESSAGE_TXNGLOBALLYCOMPLETE:
		tgc := msg.TxnGloballyComplete()
		d.ProposerDispatcher.TxnGloballyCompleteReceived(sender, &tgc)
	case msgs.MESSAGE_TOPOLOGYCHANGEREQUEST:
		// do nothing - we've just sent it to ourselves.
	case msgs.MESSAGE_MIGRATION:
		migration := msg.Migration()
		cm.Transmogrifier.MigrationReceived(sender, &migration)
	case msgs.MESSAGE_MIGRATIONCOMPLETE:
		migrationComplete := msg.MigrationComplete()
		cm.Transmogrifier.MigrationCompleteReceived(sender, &migrationComplete)
	default:
		panic(fmt.Sprintf("Unexpected message received from %v (%v)", sender, msgType))
	}
}

type connectionManagerMsg interface {
	witness() connectionManagerMsg
}

type connectionManagerMsgBasic struct{}

func (cmmb connectionManagerMsgBasic) witness() connectionManagerMsg { return cmmb }

type connectionManagerMsgShutdown chan struct{}

func (cmms connectionManagerMsgShutdown) witness() connectionManagerMsg { return cmms }

type connectionManagerMsgSetDesired struct {
	connectionManagerMsgBasic
	local  string
	remote []string
}

type connectionManagerMsgServerEstablished struct {
	connectionManagerMsgBasic
	*Connection
	send        func([]byte)
	established bool
	host        string
	rmId        common.RMId
	bootCount   uint32
	tieBreak    uint32
	clusterUUId uint64
}

type connectionManagerMsgServerLost struct {
	connectionManagerMsgBasic
	*Connection
	rmId       common.RMId
	restarting bool
}

type connectionManagerMsgClientEstablished struct {
	connectionManagerMsgBasic
	connNumber uint32
	conn       paxos.ClientConnection
	servers    map[common.RMId]paxos.Connection
	resultChan chan struct{}
}

type connectionManagerMsgServerConnAddSubscriber struct {
	connectionManagerMsgBasic
	paxos.ServerConnectionSubscriber
}

type connectionManagerMsgServerConnRemoveSubscriber struct {
	connectionManagerMsgBasic
	paxos.ServerConnectionSubscriber
}

type connectionManagerMsgSetTopology struct {
	connectionManagerMsgBasic
	topology  *configuration.Topology
	callbacks map[eng.TopologyChangeSubscriberType]func()
}

type connectionManagerMsgTopologyAddSubscriber struct {
	connectionManagerMsgBasic
	eng.TopologySubscriber
	subType    eng.TopologyChangeSubscriberType
	topology   *configuration.Topology
	resultChan chan struct{}
}

type connectionManagerMsgTopologyRemoveSubscriber struct {
	connectionManagerMsgBasic
	eng.TopologySubscriber
	subType eng.TopologyChangeSubscriberType
}

type connectionManagerMsgRequestConfigChange struct {
	connectionManagerMsgBasic
	config *configuration.Configuration
}

type connectionManagerMsgStatus struct {
	connectionManagerMsgBasic
	*server.StatusConsumer
}

func (cm *ConnectionManager) Shutdown(sync paxos.Blocking) {
	c := make(chan struct{})
	cm.enqueueSyncQuery(connectionManagerMsgShutdown(c), c)
	if sync == paxos.Sync {
		<-c
	}
}

func (cm *ConnectionManager) SetDesiredServers(localhost string, remotehosts []string) {
	cm.enqueueQuery(connectionManagerMsgSetDesired{
		local:  localhost,
		remote: remotehosts,
	})
}

func (cm *ConnectionManager) ServerEstablished(conn *Connection, host string, rmId common.RMId, bootCount uint32, tieBreak uint32, clusterUUId uint64) {
	cm.enqueueQuery(&connectionManagerMsgServerEstablished{
		Connection:  conn,
		send:        conn.Send,
		established: true,
		host:        host,
		rmId:        rmId,
		bootCount:   bootCount,
		tieBreak:    tieBreak,
		clusterUUId: clusterUUId,
	})
}

func (cm *ConnectionManager) ServerLost(conn *Connection, rmId common.RMId, restarting bool) {
	cm.enqueueQuery(connectionManagerMsgServerLost{
		Connection: conn,
		rmId:       rmId,
		restarting: restarting,
	})
}

// NB client established gets you server connection subscriber too. It
// does not get you a topology subscriber.
func (cm *ConnectionManager) ClientEstablished(connNumber uint32, conn paxos.ClientConnection) map[common.RMId]paxos.Connection {
	query := &connectionManagerMsgClientEstablished{
		connNumber: connNumber,
		conn:       conn,
		resultChan: make(chan struct{}),
	}
	if cm.enqueueSyncQuery(query, query.resultChan) {
		return query.servers
	} else {
		return nil
	}
}

func (cm *ConnectionManager) ClientLost(connNumber uint32, conn paxos.ClientConnection) {
	cm.Lock()
	delete(cm.connCountToClient, connNumber)
	cm.Unlock()
	cm.RemoveServerConnectionSubscriber(conn)
}

func (cm *ConnectionManager) GetClient(bootNumber, connNumber uint32) paxos.ClientConnection {
	if bootNumber != cm.bootcount && bootNumber != 0 {
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

func (cm *ConnectionManager) AddServerConnectionSubscriber(obs paxos.ServerConnectionSubscriber) {
	cm.enqueueQuery(connectionManagerMsgServerConnAddSubscriber{ServerConnectionSubscriber: obs})
}

func (cm *ConnectionManager) RemoveServerConnectionSubscriber(obs paxos.ServerConnectionSubscriber) {
	cm.enqueueQuery(connectionManagerMsgServerConnRemoveSubscriber{ServerConnectionSubscriber: obs})
}

func (cm *ConnectionManager) SetTopology(topology *configuration.Topology, callbacks map[eng.TopologyChangeSubscriberType]func()) {
	cm.enqueueQuery(connectionManagerMsgSetTopology{
		topology:  topology,
		callbacks: callbacks,
	})
}

func (cm *ConnectionManager) AddTopologySubscriber(subType eng.TopologyChangeSubscriberType, obs eng.TopologySubscriber) *configuration.Topology {
	query := &connectionManagerMsgTopologyAddSubscriber{
		TopologySubscriber: obs,
		subType:            subType,
		resultChan:         make(chan struct{}),
	}
	if cm.enqueueSyncQuery(query, query.resultChan) {
		return query.topology
	}
	return nil
}

func (cm *ConnectionManager) RemoveTopologySubscriberAsync(subType eng.TopologyChangeSubscriberType, obs eng.TopologySubscriber) {
	cm.enqueueQuery(connectionManagerMsgTopologyRemoveSubscriber{
		TopologySubscriber: obs,
		subType:            subType,
	})
}

func (cm *ConnectionManager) RequestConfigurationChange(config *configuration.Configuration) {
	cm.enqueueQuery(connectionManagerMsgRequestConfigChange{config: config})
}

func (cm *ConnectionManager) Status(sc *server.StatusConsumer) {
	cm.enqueueQuery(connectionManagerMsgStatus{StatusConsumer: sc})
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

func NewConnectionManager(rmId common.RMId, bootCount uint32, procs int, db *db.Databases, nodeCertPrivKeyPair *certs.NodeCertificatePrivateKeyPair, port uint16, ss ShutdownSignaller, config *configuration.Configuration) (*ConnectionManager, *TopologyTransmogrifier) {
	cm := &ConnectionManager{
		RMId:                          rmId,
		bootcount:                     bootCount,
		NodeCertificatePrivateKeyPair: nodeCertPrivKeyPair,
		servers:           make(map[string]*connectionManagerMsgServerEstablished),
		rmToServer:        make(map[common.RMId]*connectionManagerMsgServerEstablished),
		connCountToClient: make(map[uint32]paxos.ClientConnection),
		desired:           nil,
	}
	cm.serverConnSubscribers.subscribers = make(map[paxos.ServerConnectionSubscriber]server.EmptyStruct)
	cm.serverConnSubscribers.ConnectionManager = cm

	topSubs := make([]map[eng.TopologySubscriber]server.EmptyStruct, eng.TopologyChangeSubscriberTypeLimit)
	for idx := range topSubs {
		topSubs[idx] = make(map[eng.TopologySubscriber]server.EmptyStruct)
	}
	topSubs[eng.ConnectionManagerSubscriber][cm] = server.EmptyStructVal
	cm.topologySubscribers.subscribers = topSubs
	cm.topologySubscribers.ConnectionManager = cm

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
	cm.Dispatchers = paxos.NewDispatchers(cm, rmId, uint8(procs), db, lc)
	transmogrifier, localEstablished := NewTopologyTransmogrifier(db, cm, lc, port, ss, config)
	cm.Transmogrifier = transmogrifier
	go cm.actorLoop(head)
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
			case connectionManagerMsgSetDesired:
				cm.setDesiredServers(msgT)
			case *connectionManagerMsgServerEstablished:
				cm.serverEstablished(msgT)
			case connectionManagerMsgServerLost:
				cm.serverLost(msgT)
			case *connectionManagerMsgClientEstablished:
				cm.clientEstablished(msgT)
			case connectionManagerMsgSetTopology:
				cm.setTopology(msgT.topology, msgT.callbacks)
			case connectionManagerMsgServerConnAddSubscriber:
				cm.serverConnSubscribers.AddSubscriber(msgT.ServerConnectionSubscriber)
			case connectionManagerMsgServerConnRemoveSubscriber:
				cm.serverConnSubscribers.RemoveSubscriber(msgT.ServerConnectionSubscriber)
			case *connectionManagerMsgTopologyAddSubscriber:
				msgT.topology = cm.topology
				close(msgT.resultChan)
				cm.topologySubscribers.AddSubscriber(msgT.subType, msgT.TopologySubscriber)
			case connectionManagerMsgTopologyRemoveSubscriber:
				cm.topologySubscribers.RemoveSubscriber(msgT.subType, msgT.TopologySubscriber)
			case connectionManagerMsgRequestConfigChange:
				cm.Transmogrifier.RequestConfigurationChange(msgT.config)
			case connectionManagerMsgStatus:
				cm.status(msgT.StatusConsumer)
			default:
				err = fmt.Errorf("Fatal to ConnectionManager: Received unexpected message: %#v", msgT)
			}
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	if err != nil {
		panic(err)
	}
	cm.cellTail.Terminate()
	for _, cd := range cm.servers {
		cd.Shutdown(paxos.Sync)
	}
	cm.RLock()
	for _, cc := range cm.connCountToClient {
		cc.Shutdown(paxos.Sync)
	}
	cm.RUnlock()
	if shutdownChan != nil {
		close(shutdownChan)
	}
}

func (cm *ConnectionManager) setDesiredServers(hosts connectionManagerMsgSetDesired) {
	cm.desired = hosts.remote

	localHostChanged := cm.localHost != hosts.local
	cm.Lock()
	cm.localHost = hosts.local
	cm.Unlock()

	if localHostChanged {
		cd := cm.rmToServer[cm.RMId]
		delete(cm.rmToServer, cd.rmId)
		cm.serverConnSubscribers.ServerConnLost(cd.rmId)
		cd = cd.clone()
		cd.host = hosts.local
		cm.rmToServer[cd.rmId] = cd
		cm.servers[cd.host] = cd
		cm.serverConnSubscribers.ServerConnEstablished(cd)
	}

	desiredMap := make(map[string]server.EmptyStruct, len(hosts.remote))
	for _, host := range hosts.remote {
		desiredMap[host] = server.EmptyStructVal
		if _, found := cm.servers[host]; !found {
			cm.servers[host] = &connectionManagerMsgServerEstablished{
				Connection: NewConnectionToDial(host, cm),
				host:       host,
			}
		}
	}
	for host, sconn := range cm.servers {
		if _, found := desiredMap[host]; !found && !sconn.established {
			sconn.Shutdown(paxos.Async)
			delete(cm.servers, host)
		}
	}
}

func (cm *ConnectionManager) serverEstablished(connEst *connectionManagerMsgServerEstablished) {
	if cd, found := cm.servers[connEst.host]; found && cd.Connection == connEst.Connection {
		// fall through to where we do the safe insert of connEst

	} else if found {
		killOld, killNew := false, false
		switch {
		case !cd.established:
			killOld = true
		case cd.rmId != connEst.rmId:
			killOld, killNew = true, true
		case cd.bootCount < connEst.bootCount:
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
			cd.Shutdown(paxos.Async)
			if cd.established {
				delete(cm.rmToServer, cd.rmId)
				cm.serverConnSubscribers.ServerConnLost(cd.rmId)
			}
		}
		if killNew {
			connEst.Shutdown(paxos.Async)
			if killOld {
				cm.servers[cd.host] = &connectionManagerMsgServerEstablished{
					Connection: NewConnectionToDial(cd.host, cm),
					host:       cd.host,
				}
			}
			return
		}
	}

	if cd, found := cm.rmToServer[connEst.rmId]; found && connEst.rmId == cm.RMId {
		log.Printf("%v is claiming to have the same RMId as ourself! (%v)",
			connEst.host, cm.RMId)
		connEst.Shutdown(paxos.Async)
		cm.servers[connEst.host] = &connectionManagerMsgServerEstablished{
			Connection: NewConnectionToDial(connEst.host, cm),
			host:       connEst.host,
		}

	} else if found && connEst.host != cd.host {
		log.Printf("%v claimed by multiple servers: %v and %v. Recreating both connections.",
			connEst.rmId, cd.host, connEst.host)
		cd.Shutdown(paxos.Async)
		connEst.Shutdown(paxos.Async)
		delete(cm.rmToServer, cd.rmId)
		cm.serverConnSubscribers.ServerConnLost(cd.rmId)
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
		cm.serverConnSubscribers.ServerConnEstablished(connEst)
	}
}

func (cm *ConnectionManager) serverLost(connLost connectionManagerMsgServerLost) {
	rmId := connLost.rmId
	if cd, found := cm.rmToServer[connLost.rmId]; found && cd.Connection == connLost.Connection {
		log.Printf("Connection to RMId %v lost\n", rmId)
		cd.established = false
		delete(cm.rmToServer, rmId)
		if !connLost.restarting {
			if cd1, found := cm.servers[cd.host]; found && cd1 == cd {
				delete(cm.servers, cd.host)
				for _, host := range cm.desired {
					if host == cd.host {
						cm.servers[host] = &connectionManagerMsgServerEstablished{
							Connection: NewConnectionToDial(host, cm),
							host:       host,
						}
						break
					}
				}
			}
		}
		cm.serverConnSubscribers.ServerConnLost(rmId)
	}
}

func (cm *ConnectionManager) clientEstablished(msg *connectionManagerMsgClientEstablished) {
	cm.Lock()
	cm.connCountToClient[msg.connNumber] = msg.conn
	cm.Unlock()
	msg.servers = cm.cloneRMToServer()
	close(msg.resultChan)
	cm.serverConnSubscribers.AddSubscriber(msg.conn)
}

func (cm *ConnectionManager) setTopology(topology *configuration.Topology, callbacks map[eng.TopologyChangeSubscriberType]func()) {
	server.Log("Topology change:", topology)
	cm.topology = topology
	cm.topologySubscribers.TopologyChanged(topology, callbacks)
	cd := cm.rmToServer[cm.RMId]
	if clusterUUId := topology.ClusterUUId(); cd.clusterUUId == 0 && clusterUUId != 0 {
		delete(cm.rmToServer, cd.rmId)
		cm.serverConnSubscribers.ServerConnLost(cd.rmId)
		cd = cd.clone()
		cd.clusterUUId = clusterUUId
		cm.rmToServer[cm.RMId] = cd
		cm.servers[cd.host] = cd
		cm.serverConnSubscribers.ServerConnEstablished(cd)
	}
}

func (cm *ConnectionManager) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	done(true)
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
	sc.Emit(fmt.Sprintf("Boot Count: %v", cm.bootcount))
	sc.Emit(fmt.Sprintf("Current Topology: %v", cm.topology))
	if cm.topology != nil && cm.topology.Next() != nil {
		sc.Emit(fmt.Sprintf("Next Topology: %v", cm.topology.Next()))
	}
	serverConnections := make([]string, 0, len(cm.servers))
	for server := range cm.servers {
		serverConnections = append(serverConnections, server)
	}
	sc.Emit(fmt.Sprintf("ServerConnectionSubscribers: %v", len(cm.serverConnSubscribers.subscribers)))
	topSubs := make([]int, eng.TopologyChangeSubscriberTypeLimit)
	for idx, subs := range cm.topologySubscribers.subscribers {
		topSubs[idx] = len(subs)
	}
	sc.Emit(fmt.Sprintf("TopologySubscribers: %v", topSubs))
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
	cm.RLock()
	sc.Emit(fmt.Sprintf("Client Connection Count: %v", len(cm.connCountToClient)))
	cm.connCountToClient[0].(*client.LocalConnection).Status(sc.Fork())
	for _, conn := range cm.connCountToClient {
		if c, ok := conn.(*Connection); ok {
			c.Status(sc.Fork())
		}
	}
	cm.RUnlock()
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
	cm.DispatchMessage(cm.RMId, msg.Which(), msg)
}

// serverConnSubscribers
func (subs serverConnSubscribers) ServerConnEstablished(cd *connectionManagerMsgServerEstablished) {
	rmToServerCopy := subs.cloneRMToServer()
	for ob := range subs.subscribers {
		ob.ConnectionEstablished(cd.rmId, cd, rmToServerCopy)
	}
}

func (subs serverConnSubscribers) ServerConnLost(rmId common.RMId) {
	rmToServerCopy := subs.cloneRMToServer()
	for ob := range subs.subscribers {
		ob.ConnectionLost(rmId, rmToServerCopy)
	}
}

func (subs serverConnSubscribers) AddSubscriber(ob paxos.ServerConnectionSubscriber) {
	if _, found := subs.subscribers[ob]; found {
		server.Log(ob, "CM found duplicate add serverConn subscriber")
	} else {
		subs.subscribers[ob] = server.EmptyStructVal
		ob.ConnectedRMs(subs.cloneRMToServer())
	}
}

func (subs serverConnSubscribers) RemoveSubscriber(ob paxos.ServerConnectionSubscriber) {
	delete(subs.subscribers, ob)
}

// topologySubscribers
func (subs topologySubscribers) TopologyChanged(topology *configuration.Topology, callbacks map[eng.TopologyChangeSubscriberType]func()) {
	for subType, subsMap := range subs.subscribers {
		subTypeCopy := subType
		subCount := len(subsMap)
		resultChan := make(chan bool, subCount)
		done := func(success bool) { resultChan <- success }
		for sub := range subsMap {
			sub.TopologyChanged(topology, done)
		}
		if cb, found := callbacks[eng.TopologyChangeSubscriberType(subType)]; found {
			cbCopy := cb
			go func() {
				server.Log("CM TopologyChanged", subTypeCopy, "expects", subCount, "Dones")
				for subCount > 0 {
					if result := <-resultChan; result {
						subCount--
					} else {
						server.Log("CM TopologyChanged", subTypeCopy, "failed")
						return
					}
				}
				server.Log("CM TopologyChanged", subTypeCopy, "all done")
				cbCopy()
			}()
		}
	}
}

func (subs topologySubscribers) AddSubscriber(subType eng.TopologyChangeSubscriberType, ob eng.TopologySubscriber) {
	if _, found := subs.subscribers[subType][ob]; found {
		server.Log(ob, "CM found duplicate add topology subscriber")
	} else {
		subs.subscribers[subType][ob] = server.EmptyStructVal
	}
}

func (subs topologySubscribers) RemoveSubscriber(subType eng.TopologyChangeSubscriberType, ob eng.TopologySubscriber) {
	delete(subs.subscribers[subType], ob)
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

func (cd *connectionManagerMsgServerEstablished) ClusterUUId() uint64 {
	return cd.clusterUUId
}

func (cd *connectionManagerMsgServerEstablished) Send(msg []byte) {
	cd.send(msg)
}

func (cd *connectionManagerMsgServerEstablished) Shutdown(sync paxos.Blocking) {
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
		clusterUUId: cd.clusterUUId,
	}
}
