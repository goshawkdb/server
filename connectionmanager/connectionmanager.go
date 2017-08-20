package network

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/common/certs"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/network/tcpcapnproto"
	"goshawkdb.io/server/router"
	"goshawkdb.io/server/types"
	cconn "goshawkdb.io/server/types/connections/client"
	sconn "goshawkdb.io/server/types/connections/server"
	topo "goshawkdb.io/server/types/topology"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/status"
	"net"
	"sync"
)

type ShutdownSignaller interface {
	SignalShutdown()
}

type ConnectionManager struct {
	*actor.Mailbox
	*actor.BasicServerOuter

	lock                          sync.RWMutex
	parentLogger                  log.Logger
	self                          common.RMId
	localHost                     string
	bootcount                     uint32
	certificate                   []byte
	router                        *router.Router
	nodeCertificatePrivateKeyPair *certs.NodeCertificatePrivateKeyPair
	topology                      *configuration.Topology
	servers                       map[string][]*connectionManagerMsgServerEstablished
	rmToServer                    map[common.RMId]*connectionManagerMsgServerEstablished
	flushedServers                map[common.RMId]types.EmptyStruct
	connCountToClient             map[uint32]cconn.ClientConnection
	desired                       []string
	serverConnSubscribers         serverConnSubscribers
	topologySubscribers           topologySubscribers
	clientConnsGauge              prometheus.Gauge
	serverConnsGauge              prometheus.Gauge
	clientTxnMetrics              *cconn.ClientTxnMetrics

	inner connectionManagerInner
}

type connectionManagerInner struct {
	*ConnectionManager
	*actor.BasicServerInner
}

func NewConnectionManager(rmId common.RMId, bootcount uint32, certificate []byte, router *router.Router, logger log.Logger) *ConnectionManager {

	cm := &ConnectionManager{
		parentLogger:      logger,
		self:              rmId,
		bootcount:         bootcount,
		certificate:       certificate,
		router:            router,
		servers:           make(map[string][]*connectionManagerMsgServerEstablished),
		rmToServer:        make(map[common.RMId]*connectionManagerMsgServerEstablished),
		flushedServers:    make(map[common.RMId]types.EmptyStruct),
		connCountToClient: make(map[uint32]cconn.ClientConnection),
	}

	cm.serverConnSubscribers.subscribers = make(map[sconn.ServerConnectionSubscriber]types.EmptyStruct)
	cm.serverConnSubscribers.ConnectionManager = cm

	topSubs := make([]map[topo.TopologySubscriber]types.EmptyStruct, topo.TopologyChangeSubscriberTypeLimit)
	for idx := range topSubs {
		topSubs[idx] = make(map[topo.TopologySubscriber]types.EmptyStruct)
	}
	cm.topologySubscribers.subscribers = topSubs
	cm.topologySubscribers.ConnectionManager = cm

	cmi := &cm.inner
	cmi.ConnectionManager = cm
	cmi.BasicServerInner = actor.NewBasicServerInner(log.With(logger, "subsystem", "connectionManager"))

	_, err := actor.Spawn(cmi)
	if err != nil {
		panic(err) // "impossible"
	}

	// this remaining initialisation work must be done in this thread
	// because we need connectionManager actually working for this
	// (e.g. localConnection creation requires sync calls to
	// connectionManager!).
	/*
		cd := &connectionManagerMsgServerEstablished{
			send:        cmi.send,
			host:        cm.localHost,
			rmId:        cm.self,
			bootcount:   cm.bootcount,
			established: true,
			cm:          cm,
		}
		cm.rmToServer[cd.rmId] = cd
		cm.servers[cm.localHost] = []*connectionManagerMsgServerEstablished{cd}

		cm.localConnection = client.NewLocalConnection(cm.self, cm.bootcount, cm, cm.parentLogger)
		cm.Dispatchers = paxos.NewDispatchers(cm, cm.self, cm.bootcount, procs, db, cm.localConnection, cm.parentLogger)
		transmogrifier, localEstablished := topologytransmogrifier.NewTopologyTransmogrifier(db, cm, cm.localConnection, port, ss, config, cm.parentLogger)

		<-localEstablished
		return cm, transmogrifier, cm.localConnection
	*/
	return nil
}

type connectionManagerMsgServerEstablished struct {
	*sconn.ServerConnection
	established bool
	cm          *ConnectionManager
}

func (cd *connectionManagerMsgServerEstablished) clone() *connectionManagerMsgServerEstablished {
	return &connectionManagerMsgServerEstablished{
		ServerConnection: cd.ServerConnection,
		established:      cd.established,
		cm:               cd.cm,
	}
}

func (msg *connectionManagerMsgServerEstablished) Exec() (terminate bool, error error) {
	cm := msg.cm
	if cm.serverConnsGauge != nil {
		cm.serverConnsGauge.Inc()
	}

	if msg.RMId == cm.self {
		cm.inner.Logger.Log("msg", "RMId collision with ourself detected.", "RMId", msg.RMId, "remoteHost", msg.Host)
		go msg.ShutdownSync()
		return

	} else if cd, found := cm.rmToServer[msg.RMId]; found && msg.Host != cd.Host {
		cm.inner.Logger.Log("msg", "RMId collision with remote hosts detected. Restarting both connections.", "RMId", msg.RMId, "remoteHost1", cd.Host, "remoteHost2", msg.Host)
		go cd.ShutdownSync()
		go msg.ShutdownSync()
		return

	} else if !found {
		cm.rmToServer[msg.RMId] = msg
		cm.serverConnSubscribers.ServerConnEstablished(msg, msg.Flushed)
	}

	cds, found := cm.servers[msg.Host]
	if found {
		holeIdx := -1
		foundIdx := -1
		for idx, cd := range cds {
			if cd == nil && holeIdx == -1 && idx > 0 { // idx 0 is reserved for dialers
				holeIdx = idx
			} else if cd != nil && cd.ServerConnection == msg.ServerConnection {
				foundIdx = idx
				break
			}
		}

		// Due to acceptable races, we can be in a situation where we
		// think there are multiple listener connections. That's all fine.
		switch {
		case foundIdx == -1 && holeIdx == -1:
			cm.servers[msg.Host] = append(cds, msg)
		case foundIdx == -1:
			cds[holeIdx] = msg
		default: // foundIdx != -1
			cds[foundIdx] = msg
		}

	} else {
		// It's a connection we're not expecting, but maybe it's from a
		// server with a newer topology than us. So it's wrong to reject
		// this connection.
		// idx 0 is reserved for dialers, which *we* create.
		cm.servers[msg.Host] = []*connectionManagerMsgServerEstablished{nil, msg}
	}
	return
}

func (cm *ConnectionManager) ServerEstablished(server *sconn.ServerConnection) {
	cm.EnqueueMsg(&connectionManagerMsgServerEstablished{
		ServerConnection: server,
		established:      true,
		cm:               cm,
	})
}

type connectionManagerMsgServerLost struct {
	*sconn.ServerConnection
	restarting bool
	cm         *ConnectionManager
}

func (msg *connectionManagerMsgServerLost) Exec() (terminate bool, err error) {
	cm := msg.cm
	if cm.serverConnsGauge != nil {
		cm.serverConnsGauge.Dec()
	}

	rmId := msg.RMId
	host := msg.Host
	utils.DebugLog(cm.inner.Logger, "debug", "Server Connection reported down.",
		"RMId", rmId, "remoteHost", host, "restarting", msg.restarting, "desired", cm.desired)
	if cds, found := cm.servers[host]; found {
		restartingAndDesired := false
		if msg.restarting {
			// it may be restarting, but we could have changed our
			// desired servers in the mean time, so we need to look up
			// whether or not we want it to be restarting.
			for _, desiredHost := range cm.desired {
				if restartingAndDesired = desiredHost == host; restartingAndDesired {
					break
				}
			}
		}
		if restartingAndDesired { // just need to find it and set !established
			for _, cd := range cds {
				if cd != nil && cd.ServerConnection == msg.ServerConnection {
					cd.established = false
					break
				}
			}
		} else { // need to remove it completely
			allNil := true
			for idx, cd := range cds {
				if cd != nil && cd.ServerConnection == msg.ServerConnection {
					cds[idx] = nil
					if msg.restarting { // it's restarting, but we don't want it to, so kill it off
						utils.DebugLog(cm.inner.Logger, "debug", "Shutting down connection.", "RMId", rmId)
						go cd.ShutdownSync()
					}
				} else if cd != nil {
					allNil = false
				}
			}
			if allNil {
				delete(cm.servers, host)
			}
		}
	}
	if cd, found := cm.rmToServer[rmId]; found && cd.ServerConnection == msg.ServerConnection {
		cm.inner.Logger.Log("msg", "Connection lost.", "RMId", rmId)
		cd.established = false
		delete(cm.rmToServer, rmId)
		cm.serverConnSubscribers.ServerConnLost(rmId)
		if cds, found := cm.servers[host]; found {
			for _, cd := range cds {
				if cd != nil && cd.established { // backup connection found
					cm.inner.Logger.Log("msg", "Alternative connection found.", "RMId", rmId)
					cm.rmToServer[rmId] = cd
					cm.serverConnSubscribers.ServerConnEstablished(cd, cd.Flushed)
					break
				}
			}
		}
	}

	return
}

func (cm *ConnectionManager) ServerLost(server *sconn.ServerConnection, restarting bool) {
	cm.EnqueueMsg(&connectionManagerMsgServerLost{
		ServerConnection: server,
		restarting:       restarting,
		cm:               cm,
	})
}

type connectionManagerMsgServerFlushed struct {
	*ConnectionManager
	rmId common.RMId
}

func (msg *connectionManagerMsgServerFlushed) Exec() (bool, error) {
	if msg.flushedServers != nil {
		msg.flushedServers[msg.rmId] = types.EmptyStructVal
		msg.inner.checkFlushed()
	}
	return false, nil
}

func (cm *ConnectionManager) ServerConnectionFlushed(rmId common.RMId) {
	cm.EnqueueMsg(&connectionManagerMsgServerFlushed{
		ConnectionManager: cm,
		rmId:              rmId,
	})
}

type connectionManagerMsgClientEstablished struct {
	actor.MsgSyncQuery
	*ConnectionManager
	connNumber       uint32
	conn             cconn.ClientConnection
	servers          map[common.RMId]*sconn.ServerConnection
	clientTxnMetrics *cconn.ClientTxnMetrics
}

func (msg *connectionManagerMsgClientEstablished) Exec() (bool, error) {
	defer msg.MustClose()
	if msg.flushedServers == nil || msg.connNumber == 0 { // must always allow localconnection through!
		msg.lock.Lock()
		msg.connCountToClient[msg.connNumber] = msg.conn
		if msg.clientConnsGauge != nil {
			msg.clientConnsGauge.Inc()
		}
		msg.lock.Unlock()
		msg.servers = msg.cloneRMToServer()
		msg.clientTxnMetrics = msg.clientTxnMetrics
	}
	return false, nil
}

func (cm *ConnectionManager) ClientEstablished(connNumber uint32, conn cconn.ClientConnection) (map[common.RMId]*sconn.ServerConnection, *cconn.ClientTxnMetrics) {
	msg := &connectionManagerMsgClientEstablished{
		ConnectionManager: cm,
		connNumber:        connNumber,
		conn:              conn,
	}
	msg.InitMsg(cm)
	if cm.EnqueueMsg(msg) && msg.Wait() {
		return msg.servers, msg.clientTxnMetrics
	} else {
		return nil, nil
	}
}

func (cm *ConnectionManager) ClientLost(connNumber uint32, conn cconn.ClientConnection) {
	cm.lock.Lock()
	delete(cm.connCountToClient, connNumber)
	if cm.clientConnsGauge != nil {
		cm.clientConnsGauge.Dec()
	}
	cm.lock.Unlock()
}

func (cm *ConnectionManager) GetClient(bootNumber, connNumber uint32) cconn.ClientConnection {
	if bootNumber != cm.bootcount && bootNumber != 0 {
		return nil
	}
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	return cm.connCountToClient[connNumber]
}

func (cm *ConnectionManager) LocalHost() string {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	return cm.localHost
}

func (cm *ConnectionManager) NodeCertificatePrivateKeyPair() *certs.NodeCertificatePrivateKeyPair {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	return cm.nodeCertificatePrivateKeyPair
}

func (cm *ConnectionManager) AddServerConnectionSubscriber(obs sconn.ServerConnectionSubscriber) {
	cm.EnqueueFuncAsync(func() (bool, error) {
		cm.serverConnSubscribers.AddSubscriber(obs)
		return false, nil
	})
}

func (cm *ConnectionManager) RemoveServerConnectionSubscriber(obs sconn.ServerConnectionSubscriber) {
	cm.EnqueueFuncAsync(func() (bool, error) {
		cm.serverConnSubscribers.RemoveSubscriber(obs)
		return false, nil
	})
}

type connectionManagerMsgSetTopology struct {
	topology  *configuration.Topology
	callbacks map[topo.TopologyChangeSubscriberType]func()
	local     string
	remote    []string
	cm        *ConnectionManager
}

func (msg *connectionManagerMsgSetTopology) Exec() (bool, error) {
	cm := msg.cm
	utils.DebugLog(cm.inner.Logger, "debug", "Topology change.", "topology", msg.topology)
	cm.topology = msg.topology
	cm.topologySubscribers.TopologyChanged(cm.topology, msg.callbacks)
	cm.inner.checkFlushed()
	cd := cm.rmToServer[cm.self]
	if clusterUUId := cm.topology.ClusterUUId; cd.ClusterUUId == 0 && clusterUUId != 0 {
		// we now have a clusterUUId so we need to announce that
		delete(cm.rmToServer, cd.RMId)
		cm.serverConnSubscribers.ServerConnLost(cd.RMId)
		cd = cd.clone()
		cd.ClusterUUId = clusterUUId
		cm.rmToServer[cm.self] = cd
		// do *not* change this localHost to cd.Host - localhost change is taken care of later on
		cm.servers[cm.localHost][0] = cd
		cm.serverConnSubscribers.ServerConnEstablished(cd, func() { cm.ServerConnectionFlushed(cd.RMId) })
	}

	if cm.localHost != msg.local {
		oldLocalHost := cm.localHost

		host, _, err := net.SplitHostPort(msg.local)
		if err != nil {
			return false, err
		}
		ip := net.ParseIP(host)
		if ip != nil {
			host = ""
		}
		nodeCertPrivKeyPair, err := certs.GenerateNodeCertificatePrivateKeyPair(cm.certificate, host, ip, cm.topology.ClusterId)
		if err != nil {
			return false, err
		}

		cm.lock.Lock()
		cm.localHost = msg.local
		cm.nodeCertificatePrivateKeyPair = nodeCertPrivKeyPair
		cm.lock.Unlock()

		cd := cm.rmToServer[cm.self]
		delete(cm.rmToServer, cd.RMId)
		delete(cm.servers, oldLocalHost)
		cm.serverConnSubscribers.ServerConnLost(cd.RMId)
		cd = cd.clone()
		cd.Host = msg.local
		cm.rmToServer[cd.RMId] = cd
		cm.servers[msg.local] = []*connectionManagerMsgServerEstablished{cd}
		cm.serverConnSubscribers.ServerConnEstablished(cd, func() { cm.ServerConnectionFlushed(cd.RMId) })
	}

	cm.desired = msg.remote
	desiredMap := make(map[string]types.EmptyStruct, len(msg.remote))
	for _, host := range msg.remote {
		desiredMap[host] = types.EmptyStructVal
		if cds, found := cm.servers[host]; !found || len(cds) == 0 || cds[0] == nil {
			// In all cases, we need to start a dialer
			cd := &connectionManagerMsgServerEstablished{
				ServerConnection: &sconn.ServerConnection{
					Host: host,
				},
			}
			tcpcapnproto.NewConnectionTCPTLSCapnpDialer(cm.self, cm.bootcount, cm.router, cm, cd.ServerConnection, cm.parentLogger)
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
					go cd.ShutdownSync()
				}
			}
		}
	}
	return false, nil
}

func (cm *ConnectionManager) SetTopology(topology *configuration.Topology, callbacks map[topo.TopologyChangeSubscriberType]func(), localhost string, remotehosts []string) {
	cm.EnqueueMsg(&connectionManagerMsgSetTopology{
		topology:  topology,
		callbacks: callbacks,
		local:     localhost,
		remote:    remotehosts,
		cm:        cm,
	})
}

type connectionManagerMsgTopologyAddSubscriber struct {
	actor.MsgSyncQuery
	*ConnectionManager
	subType    topo.TopologyChangeSubscriberType
	subscriber topo.TopologySubscriber
	topology   *configuration.Topology
}

func (msg *connectionManagerMsgTopologyAddSubscriber) Exec() (bool, error) {
	msg.topology = msg.ConnectionManager.topology
	msg.MustClose()
	msg.topologySubscribers.AddSubscriber(msg.subType, msg.subscriber)
	return false, nil
}

func (cm *ConnectionManager) AddTopologySubscriber(subType topo.TopologyChangeSubscriberType, obs topo.TopologySubscriber) *configuration.Topology {
	msg := &connectionManagerMsgTopologyAddSubscriber{
		ConnectionManager: cm,
		subType:           subType,
		subscriber:        obs,
	}
	msg.InitMsg(cm)
	if cm.EnqueueMsg(msg) && msg.Wait() {
		return msg.topology
	} else {
		return nil
	}
}

type connectionManagerMsgTopologyRemoveSubscriber struct {
	*ConnectionManager
	subType    topo.TopologyChangeSubscriberType
	subscriber topo.TopologySubscriber
}

func (msg *connectionManagerMsgTopologyRemoveSubscriber) Exec() (bool, error) {
	msg.topologySubscribers.RemoveSubscriber(msg.subType, msg.subscriber)
	return false, nil
}

func (cm *ConnectionManager) RemoveTopologySubscriberAsync(subType topo.TopologyChangeSubscriberType, obs topo.TopologySubscriber) {
	cm.EnqueueMsg(&connectionManagerMsgTopologyRemoveSubscriber{
		ConnectionManager: cm,
		subscriber:        obs,
		subType:           subType,
	})
}

type connectionManagerMsgStatus struct {
	*ConnectionManager
	sc *status.StatusConsumer
}

func (msg *connectionManagerMsgStatus) Exec() (bool, error) {
	sc := msg.sc
	sc.Emit(fmt.Sprintf("Boot Count: %v", msg.bootcount))
	sc.Emit(fmt.Sprintf("Address: %v", msg.localHost))
	sc.Emit(fmt.Sprintf("Current Topology: %v", msg.topology))
	if msg.topology != nil && msg.topology.NextConfiguration != nil {
		sc.Emit(fmt.Sprintf("Next Topology: %v", msg.topology.NextConfiguration))
	}
	serverConnections := make([]string, 0, len(msg.servers))
	for server := range msg.servers {
		serverConnections = append(serverConnections, server)
	}
	sc.Emit(fmt.Sprintf("ServerConnectionSubscribers: %v", len(msg.serverConnSubscribers.subscribers)))
	topSubs := make([]int, topo.TopologyChangeSubscriberTypeLimit)
	for idx, subs := range msg.topologySubscribers.subscribers {
		topSubs[idx] = len(subs)
	}
	sc.Emit(fmt.Sprintf("TopologySubscribers: %v", topSubs))
	rms := make([]common.RMId, 0, len(msg.rmToServer))
	for rmId := range msg.rmToServer {
		rms = append(rms, rmId)
	}
	sc.Emit(fmt.Sprintf("Active Server RMIds: %v", rms))
	sc.Emit(fmt.Sprintf("Active Server Connections: %v", serverConnections))
	sc.Emit(fmt.Sprintf("Desired Server Connections: %v", msg.desired))
	for _, cds := range msg.servers {
		for _, cd := range cds {
			if cd != nil && cd.Status != nil {
				cd.Status(sc.Fork())
			}
		}
	}
	msg.lock.RLock()
	sc.Emit(fmt.Sprintf("Client Connection Count: %v", len(msg.connCountToClient)))
	for _, conn := range msg.connCountToClient {
		conn.Status(sc.Fork())
	}
	msg.lock.RUnlock()
	sc.Join()
	return false, nil
}

func (cm *ConnectionManager) Status(sc *status.StatusConsumer) {
	cm.EnqueueMsg(&connectionManagerMsgStatus{ConnectionManager: cm, sc: sc})
}

type connectionManagerMsgMetrics struct {
	*ConnectionManager
	client           prometheus.Gauge
	server           prometheus.Gauge
	clientTxnMetrics *cconn.ClientTxnMetrics
}

func (msg *connectionManagerMsgMetrics) Exec() (bool, error) {
	msg.lock.Lock()
	msg.clientConnsGauge = msg.client
	msg.clientConnsGauge.Set(float64(len(msg.connCountToClient)))
	msg.lock.Unlock()

	msg.serverConnsGauge = msg.server
	count := 0
	for _, cds := range msg.servers {
		for _, cd := range cds {
			if cd != nil && cd.established {
				count++
			}
		}
	}
	msg.serverConnsGauge.Set(float64(count))

	msg.ConnectionManager.clientTxnMetrics = msg.clientTxnMetrics
	return false, nil
}

func (cm *ConnectionManager) SetMetrics(client, server prometheus.Gauge, clientTxnMetrics *cconn.ClientTxnMetrics) {
	cm.EnqueueMsg(&connectionManagerMsgMetrics{
		ConnectionManager: cm,
		client:            client,
		server:            server,
		clientTxnMetrics:  clientTxnMetrics,
	})
}

func (cm *connectionManagerInner) checkFlushed() {
	if cm.flushedServers != nil && cm.topology != nil {
		requiredFlushed := len(cm.topology.Hosts) - int(cm.topology.F)
		for _, rmId := range cm.topology.RMs {
			if _, found := cm.flushedServers[rmId]; found {
				requiredFlushed--
			}
		}
		if requiredFlushed <= 0 {
			cm.Logger.Log("msg", "Ready for client connections.", "RMId", cm.self)
			cm.flushedServers = nil
		}
	}
}

func (cm *ConnectionManager) cloneRMToServer() map[common.RMId]*sconn.ServerConnection {
	rmToServerCopy := make(map[common.RMId]*sconn.ServerConnection, len(cm.rmToServer))
	for rmId, server := range cm.rmToServer {
		rmToServerCopy[rmId] = server.ServerConnection
	}
	return rmToServerCopy
}

/* TODO - suspect this moves to router.
// paxos.Connection interface to allow sending to ourself.
func (cm *connectionManagerInner) send(b []byte) {
	seg, _, err := capn.ReadFromMemoryZeroCopy(b)
	if err != nil {
		panic(fmt.Sprintf("Error in capnproto decode when sending to self! %v", err))
	}
	msg := msgs.ReadRootMessage(seg)
	cm.DispatchMessage(cm.self, msg.Which(), msg)
}
*/

func (cm *connectionManagerInner) Init(self *actor.Actor) (bool, error) {
	terminate, err := cm.BasicServerInner.Init(self)
	if terminate || err != nil {
		return terminate, err
	}

	cm.BasicServerOuter = actor.NewBasicServerOuter(self.Mailbox)
	cm.Mailbox = self.Mailbox

	return false, nil
}

func (cm *connectionManagerInner) HandleShutdown(err error) bool {
	for _, cds := range cm.servers {
		for _, cd := range cds {
			if cd != nil {
				go cd.ShutdownSync()
			}
		}
	}
	// go cm.localConnection.ShutdownSync() // TODO - move
	cm.lock.RLock()
	for _, cc := range cm.connCountToClient {
		go cc.ShutdownSync()
	}
	cm.lock.RUnlock()
	return cm.BasicServerInner.HandleShutdown(err)
}

// serverConnSubscribers

type serverConnSubscribers struct {
	*ConnectionManager
	subscribers map[sconn.ServerConnectionSubscriber]types.EmptyStruct
}

// We want this to be synchronous to the extent that two calls to this
// does not end up with msgs enqueued in a different order in
// subscribers. But we do not want to block waiting for the callback
// to be hit. So that means every subscriber needs to make the
// decision for itself as to whether it's going to block and hit the
// callback straight away, or do some async thing.
func (subs serverConnSubscribers) ServerConnEstablished(cd *connectionManagerMsgServerEstablished, callback func()) {
	rmToServerCopy := subs.cloneRMToServer()
	wg := common.NewChannelWaitGroup()
	wg.Add(1)
	for ob := range subs.subscribers {
		wg.Add(1)
		ob.ConnectionEstablished(cd.ServerConnection, rmToServerCopy, wg.Done)
	}
	// we do this because wg is edge triggered, so if subs.subscribers
	// is empty, we have to have something that goes from 1 to 0
	wg.Done()
	go func() {
		if callback != nil {
			utils.DebugLog(subs.inner.Logger, "debug", "ServerConnEstablished. Expecting callbacks.")
			wg.WaitUntilEither(subs.ConnectionManager.Mailbox.Terminated)
			callback()
		}
	}()
}

func (subs serverConnSubscribers) ServerConnLost(rmId common.RMId) {
	rmToServerCopy := subs.cloneRMToServer()
	for ob := range subs.subscribers {
		ob.ConnectionLost(rmId, rmToServerCopy)
	}
}

func (subs serverConnSubscribers) AddSubscriber(ob sconn.ServerConnectionSubscriber) {
	if _, found := subs.subscribers[ob]; found {
		utils.DebugLog(subs.inner.Logger, "debug", "Found duplicate add serverConn subscriber.", "subscriber", ob)
	} else {
		subs.subscribers[ob] = types.EmptyStructVal
		ob.ConnectedRMs(subs.cloneRMToServer())
	}
}

func (subs serverConnSubscribers) RemoveSubscriber(ob sconn.ServerConnectionSubscriber) {
	delete(subs.subscribers, ob)
}

// topologySubscribers

type topologySubscribers struct {
	*ConnectionManager
	subscribers []map[topo.TopologySubscriber]types.EmptyStruct
}

// see notes at serverConnSubscribers
func (subs topologySubscribers) TopologyChanged(topology *configuration.Topology, callbacks map[topo.TopologyChangeSubscriberType]func()) {
	for subType, subsMap := range subs.subscribers {
		subTypeCopy := subType
		resultChan := make(chan bool, len(subsMap))
		done := func(result bool) { resultChan <- result }
		for sub := range subsMap {
			sub.TopologyChanged(topology, done)
		}
		callback := callbacks[topo.TopologyChangeSubscriberType(subTypeCopy)]
		if callback == nil {
			continue
		}
		expected := len(subsMap)
		go func() {
			utils.DebugLog(subs.inner.Logger, "debug", "TopologyChanged. Expecting callbacks.",
				"type", subTypeCopy, "expected", expected)
			for expected > 0 {
				select {
				case <-subs.ConnectionManager.Mailbox.Terminated:
					return
				case success := <-resultChan:
					expected--
					if !success {
						utils.DebugLog(subs.inner.Logger, "debug", "TopologyChanged. Callback failure.", "type", subTypeCopy)
						return
					}
				}
			}
			utils.DebugLog(subs.inner.Logger, "debug", "TopologyChanged. Callback success.", "type", subTypeCopy)
			callback()
		}()
	}
}

func (subs topologySubscribers) AddSubscriber(subType topo.TopologyChangeSubscriberType, ob topo.TopologySubscriber) {
	if _, found := subs.subscribers[subType][ob]; found {
		utils.DebugLog(subs.inner.Logger, "debug", "Found duplicate add topology subscriber.", "subscriber", ob)
	} else {
		subs.subscribers[subType][ob] = types.EmptyStructVal
	}
}

func (subs topologySubscribers) RemoveSubscriber(subType topo.TopologyChangeSubscriberType, ob topo.TopologySubscriber) {
	delete(subs.subscribers[subType], ob)
}
