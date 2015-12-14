package network

import (
	"fmt"
	cc "github.com/msackman/chancell"
	"log"
	"net"
)

type Listener struct {
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(listenerMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan listenerMsg
	connectionManager *ConnectionManager
	listener          *net.TCPListener
}

type listenerMsg interface {
	listenerMsgWitness()
}

type listenerConnMsg net.TCPConn

func (lcm *listenerConnMsg) listenerMsgWitness() {}

type listenerAcceptError struct{ error }

func (lae listenerAcceptError) listenerMsgWitness() {}

type listenerMsgShutdown struct{}

func (lms *listenerMsgShutdown) listenerMsgWitness() {}

var listenerMsgShutdownInst = &listenerMsgShutdown{}

func (l *Listener) Shutdown() {
	if l.enqueueQuery(listenerMsgShutdownInst) {
		l.cellTail.Wait()
	}
}

func (l *Listener) enqueueQuery(msg listenerMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return l.enqueueQueryInner(msg, cell, f)
	}
	return l.cellTail.WithCell(f)
}

func NewListener(listenPort int, cm *ConnectionManager) (*Listener, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%v", listenPort))
	if err != nil {
		return nil, err
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}
	l := &Listener{
		connectionManager: cm,
		listener:          ln,
	}
	var head *cc.ChanCellHead
	head, l.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan listenerMsg, n)
			cell.Open = func() { l.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			l.enqueueQueryInner = func(msg listenerMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
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

	go l.acceptLoop()
	go l.actorLoop(head)
	return l, nil
}

func (l *Listener) acceptLoop() {
	for {
		conn, err := l.listener.AcceptTCP()
		if err != nil {
			l.enqueueQuery(listenerAcceptError{error: err})
			return
		}
		l.enqueueQuery((*listenerConnMsg)(conn))
	}
}

func (l *Listener) actorLoop(head *cc.ChanCellHead) {
	connectionCount := uint32(0)
	var (
		err       error
		queryChan <-chan listenerMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = l.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	for !terminate {
		if msg, ok := <-queryChan; ok {
			switch msgT := msg.(type) {
			case *listenerMsgShutdown:
				terminate = true
			case listenerAcceptError:
				err = msgT
			case *listenerConnMsg:
				connectionCount++
				NewConnectionFromTCPConn((*net.TCPConn)(msgT), l.connectionManager, connectionCount)
			}
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	if err != nil {
		log.Println("Listen error:", err)
	}
	l.cellTail.Terminate()
	l.listener.Close()
}
