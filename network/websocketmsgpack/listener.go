package websocketmsgpack

import (
	"encoding/hex"
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	ghttp "goshawkdb.io/server/network/http"
	eng "goshawkdb.io/server/txnengine"
	"goshawkdb.io/server/types/connectionmanager"
	"net/http"
	"sync"
	"sync/atomic"
)

type WebsocketListener struct {
	*actor.Mailbox
	*actor.BasicServerOuter

	parentLogger      log.Logger
	connectionManager connectionmanager.ConnectionManager
	mux               *ghttp.HttpListenerWithMux
	topology          *configuration.Topology
	topologyLock      sync.RWMutex

	inner websocketListenerInner
}

type websocketListenerInner struct {
	*WebsocketListener
	*actor.BasicServerInner
}

func NewWebsocketListener(mux *ghttp.HttpListenerWithMux, cm connectionmanager.ConnectionManager, logger log.Logger) *WebsocketListener {
	l := &WebsocketListener{
		parentLogger:      logger,
		connectionManager: cm,
		mux:               mux,
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

	topology := l.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, l)
	l.putTopology(topology)
	l.configureMux()
	return false, nil
}

func (l *websocketListenerInner) HandleShutdown(err error) bool {
	l.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, l)
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
			wsHandler(l.connectionManager, connNumber, w, req, peerCerts, roots, l.parentLogger)
		} else {
			l.Logger.Log("type", "client", "authentication", "failure")
			w.WriteHeader(http.StatusForbidden)
		}
	})
	l.mux.Done()
}
