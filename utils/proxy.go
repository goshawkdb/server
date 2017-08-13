package utils

import (
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/server/types"
)

type serverConnectionPublisherProxy struct {
	logger   log.Logger
	exe      types.EnqueueActor
	upstream types.ServerConnectionPublisher
	subs     map[types.ServerConnectionSubscriber]EmptyStruct
	servers  map[common.RMId]types.ServerConnection
}

// This assumes that AddServerConnectionSubscriber and
// RemoveServerConnectionSubscriber will always be run from exe's
// thread. This uses exe to process calls from ConnectedRMs,
// ConnectionLost and ConnectionEstablished, invoked by the upstream.
func NewServerConnectionPublisherProxy(exe types.EnqueueActor, upstream types.ServerConnectionPublisher, logger log.Logger) types.ServerConnectionPublisher {
	proxy := &serverConnectionPublisherProxy{
		logger:   logger,
		exe:      exe,
		upstream: upstream,
		subs:     make(map[types.ServerConnectionSubscriber]EmptyStruct),
	}
	upstream.AddServerConnectionSubscriber(proxy)
	return proxy
}

func (proxy *serverConnectionPublisherProxy) AddServerConnectionSubscriber(obs types.ServerConnectionSubscriber) {
	proxy.subs[obs] = EmptyStructVal
	if proxy.servers != nil {
		obs.ConnectedRMs(proxy.servers)
	}
}

func (proxy *serverConnectionPublisherProxy) RemoveServerConnectionSubscriber(obs types.ServerConnectionSubscriber) {
	delete(proxy.subs, obs)
}

func (proxy *serverConnectionPublisherProxy) ConnectedRMs(servers map[common.RMId]types.ServerConnection) {
	proxy.exe.EnqueueFuncAsync(func() (bool, error) {
		proxy.servers = servers
		for sub := range proxy.subs {
			sub.ConnectedRMs(servers)
		}
		return false, nil
	})
}

func (proxy *serverConnectionPublisherProxy) ConnectionLost(lost common.RMId, servers map[common.RMId]types.ServerConnection) {
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
	gained  common.RMId
	conn    types.ServerConnection
	servers map[common.RMId]types.ServerConnection
	wg      *common.ChannelWaitGroup
}

func (msg *scppConnectionEstablished) Exec() (bool, error) {
	msg.proxy.servers = msg.servers
	for sub := range msg.proxy.subs {
		msg.wg.Add(1)
		sub.ConnectionEstablished(msg.gained, msg.conn, msg.servers, msg.wg.Done)
	}
	DebugLog(msg.proxy.logger, "debug", "ServerConnEstablished Proxy expecting callbacks.")
	msg.wg.Done() // see comment below
	return false, nil
}

func (proxy *serverConnectionPublisherProxy) ConnectionEstablished(gained common.RMId, conn types.ServerConnection, servers map[common.RMId]types.ServerConnection, onDone func()) {
	msg := &scppConnectionEstablished{
		proxy:   proxy,
		gained:  gained,
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
