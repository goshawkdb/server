package websocketmsgpack

import (
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/network"
	ghttp "goshawkdb.io/server/network/http"
	"goshawkdb.io/server/types/connectionmanager"
	"goshawkdb.io/server/types/localconnection"
	"goshawkdb.io/server/types/topology"
	"net/http"
	"sync"
	"sync/atomic"
)

type WebsocketListener struct {
	*actor.Mailbox
	*actor.BasicServerOuter

	parentLogger      log.Logger
	self              common.RMId
	bootCount         uint32
	connectionManager connectionmanager.ConnectionManager
	mux               *ghttp.HttpListenerWithMux
	topology          *configuration.Topology
	topologyLock      sync.RWMutex
	localConnection   localconnection.LocalConnection

	inner websocketListenerInner
}

type websocketListenerInner struct {
	*WebsocketListener
	*actor.BasicServerInner
}

func NewWebsocketListener(mux *ghttp.HttpListenerWithMux, rmId common.RMId, bootCount uint32, cm connectionmanager.ConnectionManager, localConnection localconnection.LocalConnection, logger log.Logger) *WebsocketListener {
	l := &WebsocketListener{
		parentLogger:      logger,
		self:              rmId,
		bootCount:         bootCount,
		connectionManager: cm,
		mux:               mux,
		localConnection:   localConnection,
	}

	li := &l.inner
	li.WebsocketListener = l
	li.BasicServerInner = actor.NewBasicServerInner(log.With(logger, "subsystem", "websocketListener"))

	_, err := actor.Spawn(li)
	if err != nil {
		panic(err) // "impossible"
	}

	return l
}

func (l *WebsocketListener) putTopology(topology *configuration.Topology) {
	l.topologyLock.Lock()
	defer l.topologyLock.Unlock()
	l.topology = topology
}

func (l *WebsocketListener) getTopology() *configuration.Topology {
	l.topologyLock.RLock()
	defer l.topologyLock.RUnlock()
	return l.topology
}

type websocketListenerMsgTopologyChanged struct {
	actor.MsgSyncQuery
	*WebsocketListener
	topology *configuration.Topology
}

func (msg websocketListenerMsgTopologyChanged) Exec() (bool, error) {
	msg.putTopology(msg.topology)
	msg.MustClose()
	return false, nil
}

func (l *WebsocketListener) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	msg := &websocketListenerMsgTopologyChanged{WebsocketListener: l, topology: topology}
	msg.InitMsg(l)
	if l.EnqueueMsg(msg) {
		go func() {
			msg.Wait()
			done(true) // connection drop is not a problem
		}()
	} else {
		done(true) // connection drop is not a problem
	}
}

func (l *websocketListenerInner) Init(self *actor.Actor) (bool, error) {
	terminate, err := l.BasicServerInner.Init(self)
	if terminate || err != nil {
		return terminate, err
	}

	l.Mailbox = self.Mailbox
	l.BasicServerOuter = actor.NewBasicServerOuter(self.Mailbox)

	topology := l.connectionManager.AddTopologySubscriber(topology.ConnectionSubscriber, l)
	l.putTopology(topology)
	l.configureMux()
	return false, nil
}

func (l *websocketListenerInner) HandleShutdown(err error) bool {
	l.connectionManager.RemoveTopologySubscriberAsync(topology.ConnectionSubscriber, l)
	return l.BasicServerInner.HandleShutdown(err)
}

func (l *websocketListenerInner) configureMux() {
	connCount := uint32(0)

	l.mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		peerCerts := req.TLS.PeerCertificates
		if authenticated, _, _ := l.getTopology().VerifyPeerCerts(peerCerts); authenticated {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte(fmt.Sprintf("GoshawkDB Server version %v. Websocket available at /ws", server.ServerVersion)))
		} else {
			l.Logger.Log("type", "client", "authentication", "failure")
			w.WriteHeader(http.StatusForbidden)
		}
	})
	l.mux.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
		peerCerts := req.TLS.PeerCertificates
		if authenticated, hashsum, roots := l.getTopology().VerifyPeerCerts(peerCerts); authenticated {
			connNumber := 2*atomic.AddUint32(&connCount, 1) + 1
			l.Logger.Log("type", "client", "authentication", "success", "fingerprint", hex.EncodeToString(hashsum[:]), "connNumber", connNumber)
			l.wsHandler(connNumber, w, req, peerCerts, roots)
		} else {
			l.Logger.Log("type", "client", "authentication", "failure")
			w.WriteHeader(http.StatusForbidden)
		}
	})
	l.mux.Done()
}

func (l *websocketListenerInner) wsHandler(connNumber uint32, w http.ResponseWriter, r *http.Request, peerCerts []*x509.Certificate, roots map[string]common.Capability) {
	logger := log.With(l.parentLogger, "subsystem", "connection", "dir", "incoming", "protocol", "websocket", "type", "client", "connNumber", connNumber)
	c, err := upgrader.Upgrade(w, r, nil)
	if err == nil {
		logger.Log("msg", "WSS upgrade success.", "remoteHost", c.RemoteAddr())
		yesman := &wssMsgPackClient{
			logger:            logger,
			remoteHost:        fmt.Sprintf("%v", c.RemoteAddr()),
			connectionNumber:  connNumber,
			self:              l.self,
			bootCount:         l.bootCount,
			socket:            c,
			peerCerts:         peerCerts,
			roots:             roots,
			connectionManager: l.connectionManager,
			localConnection:   l.localConnection,
		}
		network.NewConnection(yesman, l.connectionManager, logger)

	} else {
		logger.Log("msg", "WSS upgrade failure.", "remoteHost", r.RemoteAddr, "error", err)
	}
}
