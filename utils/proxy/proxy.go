package proxy

import (
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/server/types"
	"goshawkdb.io/server/types/actor"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
)

type serverConnectionPublisherProxy struct {
	logger   log.Logger
	exe      actor.EnqueueActor
	upstream sconn.ServerConnectionPublisher
	subs     map[sconn.ServerConnectionSubscriber]types.EmptyStruct
	servers  map[common.RMId]*sconn.ServerConnection
}

// This assumes that AddServerConnectionSubscriber and
// RemoveServerConnectionSubscriber will always be run from exe's
// thread. This uses exe to process calls from ConnectedRMs,
// ConnectionLost and ConnectionEstablished, invoked by the upstream.
func NewServerConnectionPublisherProxy(exe actor.EnqueueActor, upstream sconn.ServerConnectionPublisher, logger log.Logger) sconn.ServerConnectionPublisher {
	proxy := &serverConnectionPublisherProxy{
		logger:   logger,
		exe:      exe,
		upstream: upstream,
		subs:     make(map[sconn.ServerConnectionSubscriber]types.EmptyStruct),
	}
	upstream.AddServerConnectionSubscriber(proxy)
	return proxy
}

func (proxy *serverConnectionPublisherProxy) AddServerConnectionSubscriber(obs sconn.ServerConnectionSubscriber) {
	proxy.subs[obs] = types.EmptyStructVal
	if proxy.servers != nil {
		obs.ConnectedRMs(proxy.servers)
	}
}

func (proxy *serverConnectionPublisherProxy) RemoveServerConnectionSubscriber(obs sconn.ServerConnectionSubscriber) {
	delete(proxy.subs, obs)
}

func (proxy *serverConnectionPublisherProxy) ConnectedRMs(servers map[common.RMId]*sconn.ServerConnection) {
	proxy.exe.EnqueueFuncAsync(func() (bool, error) {
		proxy.servers = servers
		for sub := range proxy.subs {
			sub.ConnectedRMs(servers)
		}
		return false, nil
	})
}

func (proxy *serverConnectionPublisherProxy) ConnectionLost(lost common.RMId, servers map[common.RMId]*sconn.ServerConnection) {
	proxy.exe.EnqueueFuncAsync(func() (bool, error) {
		proxy.servers = servers
		for sub := range proxy.subs {
			sub.ConnectionLost(lost, servers)
		}
		return false, nil
	})
}

type scppConnectionEstablished struct {
	proxy   *serverConnectionPublisherProxy
	conn    *sconn.ServerConnection
	servers map[common.RMId]*sconn.ServerConnection
	wg      *common.ChannelWaitGroup
}

func (msg *scppConnectionEstablished) Exec() (bool, error) {
	msg.proxy.servers = msg.servers
	for sub := range msg.proxy.subs {
		msg.wg.Add(1)
		sub.ConnectionEstablished(msg.conn, msg.servers, msg.wg.Done)
	}
	utils.DebugLog(msg.proxy.logger, "debug", "ServerConnEstablished Proxy expecting callbacks.")
	msg.wg.Done() // see comment below
	return false, nil
}

func (proxy *serverConnectionPublisherProxy) ConnectionEstablished(conn *sconn.ServerConnection, servers map[common.RMId]*sconn.ServerConnection, onDone func()) {
	msg := &scppConnectionEstablished{
		proxy:   proxy,
		conn:    conn,
		servers: servers,
		wg:      common.NewChannelWaitGroup(),
	}
	// we do this because wg is edge triggered, so if proxy.subs is
	// empty, we have to have something that goes from 1 to 0
	msg.wg.Add(1)
	if proxy.exe.EnqueueMsg(msg) { // just because it was enqueued doesn't mean it'll be exec'd
		go func() {
			msg.wg.WaitUntilEither(msg.proxy.exe.TerminatedChan())
			onDone()
		}()
	} else {
		onDone()
	}
}
