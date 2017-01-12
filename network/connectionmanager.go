package network

import (
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
	"goshawkdb.io/server/stats"
	eng "goshawkdb.io/server/txnengine"
	"log"
	"net"
	"sync"
)

type ShutdownSignaller interface {
	SignalShutdown()
}

type ConnectionManager struct {
	sync.RWMutex
	localHost                     string
	RMId                          common.RMId
	BootCount                     uint32
	certificate                   []byte
	nodeCertificatePrivateKeyPair *certs.NodeCertificatePrivateKeyPair
	Transmogrifier                *TopologyTransmogrifier
	topology                      *configuration.Topology
	cellTail                      *cc.ChanCellTail
	enqueueQueryInner             func(connectionManagerMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan                     <-chan connectionManagerMsg
	servers                       map[string][]*connectionManagerMsgServerEstablished
	rmToServer                    map[common.RMId]*connectionManagerMsgServerEstablished
	flushedServers                map[common.RMId]server.EmptyStruct
	connCountToClient             map[uint32]paxos.ClientConnection
	desired                       []string
	serverConnSubscribers         serverConnSubscribers
	topologySubscribers           topologySubscribers
	Dispatchers                   *paxos.Dispatchers
	localConnection               *client.LocalConnection
}

type serverConnSubscribers struct {
	*ConnectionManager
	subscribers map[paxos.ServerConnectionSubscriber]server.EmptyStruct
}

type topologySubscribers struct {
	*ConnectionManager
	subscribers []map[eng.TopologySubscriber]server.EmptyStruct
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
		connNumber := txnId.ConnectionCount()
		bootNumber := txnId.BootCount()
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
	case msgs.MESSAGE_FLUSHED:
		cm.ServerConnectionFlushed(sender)
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

type connectionManagerMsgServerEstablished struct {
	connectionManagerMsgBasic
	*Connection
	send          func([]byte)
	host          string
	rmId          common.RMId
	bootCount     uint32
	clusterUUId   uint64
	flushCallback func()
	established   bool
}

type connectionManagerMsgServerLost struct {
	connectionManagerMsgBasic
	*Connection
	host       string
	rmId       common.RMId
	restarting bool
}

type connectionManagerMsgServerFlushed struct {
	connectionManagerMsgBasic
	rmId common.RMId
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
	local     string
	remote    []string
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

func (cm *ConnectionManager) ServerEstablished(conn *Connection, host string, rmId common.RMId, bootCount uint32, clusterUUId uint64, flushCallback func()) {
	cm.enqueueQuery(&connectionManagerMsgServerEstablished{
		Connection:    conn,
		send:          conn.Send,
		host:          host,
		rmId:          rmId,
		bootCount:     bootCount,
		clusterUUId:   clusterUUId,
		flushCallback: flushCallback,
		established:   true,
	})
}

func (cm *ConnectionManager) ServerLost(conn *Connection, host string, rmId common.RMId, restarting bool) {
	cm.enqueueQuery(connectionManagerMsgServerLost{
		Connection: conn,
		host:       host,
		rmId:       rmId,
		restarting: restarting,
	})
}

func (cm *ConnectionManager) ServerConnectionFlushed(rmId common.RMId) {
	cm.enqueueQuery(connectionManagerMsgServerFlushed{
		rmId: rmId,
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

func (cm *ConnectionManager) NodeCertificatePrivateKeyPair() *certs.NodeCertificatePrivateKeyPair {
	cm.RLock()
	defer cm.RUnlock()
	return cm.nodeCertificatePrivateKeyPair
}

func (cm *ConnectionManager) AddServerConnectionSubscriber(obs paxos.ServerConnectionSubscriber) {
	cm.enqueueQuery(connectionManagerMsgServerConnAddSubscriber{ServerConnectionSubscriber: obs})
}

func (cm *ConnectionManager) RemoveServerConnectionSubscriber(obs paxos.ServerConnectionSubscriber) {
	cm.enqueueQuery(connectionManagerMsgServerConnRemoveSubscriber{ServerConnectionSubscriber: obs})
}

func (cm *ConnectionManager) SetTopology(topology *configuration.Topology, callbacks map[eng.TopologyChangeSubscriberType]func(), localhost string, remotehosts []string) {
	cm.enqueueQuery(connectionManagerMsgSetTopology{
		topology:  topology,
		callbacks: callbacks,
		local:     localhost,
		remote:    remotehosts,
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

func NewConnectionManager(rmId common.RMId, bootCount uint32, procs int, db *db.Databases, certificate []byte, port uint16, ss ShutdownSignaller, config *configuration.Configuration) (*ConnectionManager, *TopologyTransmogrifier, *stats.StatsPublisher) {
	cm := &ConnectionManager{
		localHost:         "",
		RMId:              rmId,
		BootCount:         bootCount,
		certificate:       certificate,
		servers:           make(map[string][]*connectionManagerMsgServerEstablished),
		rmToServer:        make(map[common.RMId]*connectionManagerMsgServerEstablished),
		flushedServers:    make(map[common.RMId]server.EmptyStruct),
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
		host:        cm.localHost,
		rmId:        rmId,
		bootCount:   bootCount,
		established: true,
	}
	cm.rmToServer[cd.rmId] = cd
	cm.servers[cm.localHost] = []*connectionManagerMsgServerEstablished{cd}
	lc := client.NewLocalConnection(rmId, bootCount, cm)
	cm.localConnection = lc
	cm.Dispatchers = paxos.NewDispatchers(cm, rmId, bootCount, uint8(procs), db, lc)
	sp := stats.NewStatsPublisher(lc)
	transmogrifier, localEstablished := NewTopologyTransmogrifier(db, cm, lc, sp, port, ss, config)
	cm.Transmogrifier = transmogrifier
	go cm.actorLoop(head)
	<-localEstablished
	return cm, transmogrifier, sp
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
			case *connectionManagerMsgServerEstablished:
				cm.serverEstablished(msgT)
			case connectionManagerMsgServerLost:
				cm.serverLost(msgT)
			case connectionManagerMsgServerFlushed:
				cm.serverFlushed(msgT.rmId)
			case *connectionManagerMsgClientEstablished:
				cm.clientEstablished(msgT)
			case connectionManagerMsgSetTopology:
				cm.setTopology(msgT.topology, msgT.callbacks)
				err = cm.setDesiredServers(msgT.local, msgT.remote)
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
		log.Fatalf("ConnectionManager encountered an error: %v", err)
	}
	cm.cellTail.Terminate()
	for _, cds := range cm.servers {
		for _, cd := range cds {
			if cd != nil {
				cd.Shutdown(paxos.Sync)
			}
		}
	}
	cm.localConnection.Shutdown(paxos.Sync)
	cm.RLock()
	for _, cc := range cm.connCountToClient {
		cc.Shutdown(paxos.Sync)
	}
	cm.RUnlock()
	if shutdownChan != nil {
		close(shutdownChan)
	}
}

func (cm *ConnectionManager) serverEstablished(connEst *connectionManagerMsgServerEstablished) {
	if connEst.rmId == cm.RMId {
		log.Printf("%v is claiming to have the same RMId as ourself! (%v)", connEst.host, cm.RMId)
		connEst.Shutdown(paxos.Async)
		return

	} else if cd, found := cm.rmToServer[connEst.rmId]; found && connEst.host != cd.host {
		log.Printf("%v claimed by multiple servers: %v and %v. Recreating both connections.",
			connEst.rmId, cd.host, connEst.host)
		cd.Shutdown(paxos.Async)
		connEst.Shutdown(paxos.Async)
		delete(cm.rmToServer, cd.rmId)
		cm.serverConnSubscribers.ServerConnLost(cd.rmId)
		return

	} else if !found {
		cm.rmToServer[connEst.rmId] = connEst
		cm.serverConnSubscribers.ServerConnEstablished(connEst, connEst.flushCallback)
	}

	cds, found := cm.servers[connEst.host]
	if found {
		holeIdx := -1
		foundIdx := -1
		for idx, cd := range cds {
			if cd == nil && holeIdx == -1 && idx > 0 { // idx 0 is reserved for dialers
				holeIdx = idx
			} else if cd != nil && cd.Connection == connEst.Connection {
				foundIdx = idx
				break
			}
		}

		// Due to acceptable races, we can be in a situation where we
		// think there are multiple listener connections. That's all fine.
		switch {
		case foundIdx == -1 && holeIdx == -1:
			cds = append(cds, connEst)
			cm.servers[connEst.host] = cds
		case foundIdx == -1:
			cds[holeIdx] = connEst
		default: // foundIdx != -1
			cds[foundIdx] = connEst
		}

	} else {
		// It's a connection we're not expecting, but maybe it's from a
		// server with a newer topology than us. So it's wrong to reject
		// this connection.
		cds = make([]*connectionManagerMsgServerEstablished, 2)
		// idx 0 is reserved for dialers, which *we* create.
		cds[1] = connEst
		cm.servers[connEst.host] = cds
	}
}

func (cm *ConnectionManager) serverLost(connLost connectionManagerMsgServerLost) {
	rmId := connLost.rmId
	host := connLost.host
	if cds, found := cm.servers[host]; found {
		if connLost.restarting { // just need to find it and set !established
			for _, cd := range cds {
				if cd != nil && cd.Connection == connLost.Connection {
					cd.established = false
					break
				}
			}
		} else { // need to remove it completely
			allNil := true
			for idx, cd := range cds {
				if cd != nil && cd.Connection == connLost.Connection {
					cds[idx] = nil
				} else if cd != nil {
					allNil = false
				}
			}
			if allNil {
				delete(cm.servers, host)
			}
		}
	}
	if cd, found := cm.rmToServer[rmId]; found && cd.Connection == connLost.Connection {
		log.Printf("Connection to %v lost\n", rmId)
		cd.established = false
		delete(cm.rmToServer, rmId)
		cm.serverConnSubscribers.ServerConnLost(rmId)
		if cds, found := cm.servers[host]; found {
			for _, cd := range cds {
				if cd != nil && cd.established { // backup connection found
					log.Printf("Alternative connection to %v found\n", rmId)
					cm.rmToServer[rmId] = cd
					cm.serverConnSubscribers.ServerConnEstablished(cd, cd.flushCallback)
					break
				}
			}
		}
	}
}

func (cm *ConnectionManager) serverFlushed(rmId common.RMId) {
	if cm.flushedServers != nil {
		cm.flushedServers[rmId] = server.EmptyStructVal
		cm.checkFlushed(cm.topology)
	}
}

func (cm *ConnectionManager) clientEstablished(msg *connectionManagerMsgClientEstablished) {
	if cm.flushedServers == nil || msg.connNumber == 0 { // must always allow localconnection through!
		cm.Lock()
		cm.connCountToClient[msg.connNumber] = msg.conn
		cm.Unlock()
		msg.servers = cm.cloneRMToServer()
		close(msg.resultChan)
		cm.serverConnSubscribers.AddSubscriber(msg.conn)
	} else {
		close(msg.resultChan)
	}
}

func (cm *ConnectionManager) setTopology(topology *configuration.Topology, callbacks map[eng.TopologyChangeSubscriberType]func()) {
	server.Log("Topology change:", topology)
	cm.topology = topology
	cm.topologySubscribers.TopologyChanged(topology, callbacks)
	cd := cm.rmToServer[cm.RMId]
	if clusterUUId := topology.ClusterUUId; cd.clusterUUId == 0 && clusterUUId != 0 {
		delete(cm.rmToServer, cd.rmId)
		cm.serverConnSubscribers.ServerConnLost(cd.rmId)
		cd = cd.clone()
		cd.clusterUUId = clusterUUId
		cm.rmToServer[cm.RMId] = cd
		// do *not* change this localHost to cd.host - localhost change is taken care of in setDesiredServers
		cm.servers[cm.localHost][0] = cd
		cm.serverConnSubscribers.ServerConnEstablished(cd, func() { cm.ServerConnectionFlushed(cd.rmId) })
	}
}

func (cm *ConnectionManager) setDesiredServers(localHost string, remote []string) error {
	cm.desired = remote

	if cm.localHost != localHost {
		oldLocalHost := cm.localHost

		host, _, err := net.SplitHostPort(localHost)
		if err != nil {
			return err
		}
		nodeCertPrivKeyPair, err := certs.GenerateNodeCertificatePrivateKeyPair(cm.certificate, host, cm.topology.ClusterId)
		if err != nil {
			return err
		}
		cm.Lock()
		cm.localHost = localHost
		cm.nodeCertificatePrivateKeyPair = nodeCertPrivKeyPair
		cm.Unlock()

		cd := cm.rmToServer[cm.RMId]
		delete(cm.rmToServer, cd.rmId)
		delete(cm.servers, oldLocalHost)
		cm.serverConnSubscribers.ServerConnLost(cd.rmId)
		cd = cd.clone()
		cd.host = localHost
		cm.rmToServer[cd.rmId] = cd
		cm.servers[localHost] = []*connectionManagerMsgServerEstablished{cd}
		cm.serverConnSubscribers.ServerConnEstablished(cd, func() { cm.ServerConnectionFlushed(cd.rmId) })
	}

	desiredMap := make(map[string]server.EmptyStruct, len(remote))
	for _, host := range remote {
		desiredMap[host] = server.EmptyStructVal
		if cds, found := cm.servers[host]; !found || len(cds) == 0 || cds[0] == nil {
			// In all cases, we need to start a dialer
			cd := &connectionManagerMsgServerEstablished{
				Connection: NewConnectionTCPTLSCapnpDialer(host, cm),
				host:       host,
			}
			if !found || len(cds) == 0 {
				cds := make([]*connectionManagerMsgServerEstablished, 1, 2)
				cds[0] = cd
				cm.servers[host] = cds
			} else {
				cds[0] = cd
			}
		}
	}
	// The intention here is to shutdown any dialers that are trying to
	// connect to hosts that are no longer desired. There is a
	// possibility the connection is actually now established and such
	// a message is waiting in our own queue. This is fine because
	// we've managed to get to this point without needing that
	// connection anyway. We don't shutdown established connections
	// though because we have the possibility that the remote end of
	// the established connection is lagging behind us. If we shutdown
	// then it could just recreate the connection in order to try to
	// catch up. So we leave the connection up and allow the remote end
	// to choose when to shut it down itself.
	for host, cds := range cm.servers {
		if host == cm.localHost {
			continue
		}
		if _, found := desiredMap[host]; !found {
			delete(cm.servers, host)
			for _, cd := range cds {
				if cd != nil && !cd.established {
					cd.Shutdown(paxos.Async)
				}
			}
		}
	}
	return nil
}

func (cm *ConnectionManager) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	cm.checkFlushed(topology)
	done(true)
}

func (cm *ConnectionManager) checkFlushed(topology *configuration.Topology) {
	if cm.flushedServers != nil && topology != nil {
		requiredFlushed := len(topology.Hosts) - int(topology.F)
		for _, rmId := range topology.RMs {
			if _, found := cm.flushedServers[rmId]; found {
				requiredFlushed--
			}
		}
		if requiredFlushed <= 0 {
			log.Printf("%v Ready for client connections.", cm.RMId)
			cm.flushedServers = nil
		}
	}
}

func (cm *ConnectionManager) cloneRMToServer() map[common.RMId]paxos.Connection {
	rmToServerCopy := make(map[common.RMId]paxos.Connection, len(cm.rmToServer))
	for rmId, server := range cm.rmToServer {
		rmToServerCopy[rmId] = server
	}
	return rmToServerCopy
}

func (cm *ConnectionManager) status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("Boot Count: %v", cm.BootCount))
	sc.Emit(fmt.Sprintf("Address: %v", cm.localHost))
	sc.Emit(fmt.Sprintf("Current Topology: %v", cm.topology))
	if cm.topology != nil && cm.topology.NextConfiguration != nil {
		sc.Emit(fmt.Sprintf("Next Topology: %v", cm.topology.NextConfiguration))
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
	for _, cds := range cm.servers {
		for _, cd := range cds {
			if cd != nil && cd.Connection != nil {
				cd.Connection.Status(sc.Fork())
			}
		}
	}
	cm.RLock()
	sc.Emit(fmt.Sprintf("Client Connection Count: %v", len(cm.connCountToClient)))
	for _, conn := range cm.connCountToClient {
		conn.Status(sc.Fork())
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
func (subs serverConnSubscribers) ServerConnEstablished(cd *connectionManagerMsgServerEstablished, callback func()) {
	rmToServerCopy := subs.cloneRMToServer()
	// we cope with the possibility that subscribers can change during iteration
	resultChan := make(chan server.EmptyStruct, len(subs.subscribers))
	done := func() { resultChan <- server.EmptyStructVal }
	expected := 0
	for ob := range subs.subscribers {
		expected++
		ob.ConnectionEstablished(cd.rmId, cd, rmToServerCopy, done)
	}
	go func() {
		for expected > 0 {
			<-resultChan
			expected--
		}
		callback()
	}()
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
	// again, we try to cope with the possibility that subsMap changes during iteration
	for subType, subsMap := range subs.subscribers {
		subTypeCopy := subType
		resultChan := make(chan bool, len(subsMap))
		done := func(success bool) { resultChan <- success }
		expected := 0
		for sub := range subsMap {
			expected++
			sub.TopologyChanged(topology, done)
		}
		if cb, found := callbacks[eng.TopologyChangeSubscriberType(subType)]; found {
			cbCopy := cb
			go func() {
				server.Log("CM TopologyChanged", subTypeCopy, "expects", expected, "Dones")
				for expected > 0 {
					if result := <-resultChan; result {
						expected--
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
		host:        cd.host,
		rmId:        cd.rmId,
		bootCount:   cd.bootCount,
		clusterUUId: cd.clusterUUId,
	}
}
