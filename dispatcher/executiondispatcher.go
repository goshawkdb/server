package dispatcher

import (
	cc "github.com/msackman/chancell"
	"log"
)

type Dispatcher struct {
	ExecutorCount uint8
	Executors     []*Executor
}

func (dis *Dispatcher) Init(count uint8) {
	executors := make([]*Executor, count)
	for idx := range executors {
		executors[idx] = newExecutor()
	}
	dis.Executors = executors
	dis.ExecutorCount = count
}

func (dis *Dispatcher) Shutdown() {
	for _, exe := range dis.Executors {
		exe.shutdown()
	}
}

type executorQuery interface {
	witness() executorQuery
}

type executorQueryBasic struct{}

func (eqb executorQueryBasic) witness() executorQuery { return eqb }

type shutdownQuery struct{ executorQueryBasic }

type applyQuery func()

func (aq applyQuery) witness() executorQuery { return aq }

type Executor struct {
	cellTail  *cc.ChanCellTail
	enqueue   func(executorQuery, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan <-chan executorQuery
}

func newExecutor() *Executor {
	exe := &Executor{}
	var head *cc.ChanCellHead
	head, exe.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan executorQuery, n)
			cell.Open = func() { exe.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			exe.enqueue = func(msg executorQuery, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
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
	go exe.loop(head)
	return exe
}

func (exe *Executor) loop(head *cc.ChanCellHead) {
	terminate := false
	var (
		queryChan <-chan executorQuery
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = exe.queryChan, cell }
	head.WithCell(chanFun)
	for !terminate {
		if msg, ok := <-queryChan; ok {
			switch query := msg.(type) {
			case shutdownQuery:
				terminate = true
			case applyQuery:
				query()
			default:
				log.Printf("Fatal to Executor: Received unexpected message: %#v", query)
				terminate = true
			}
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	exe.cellTail.Terminate()
}

func (exe *Executor) send(msg executorQuery) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return exe.enqueue(msg, cell, f)
	}
	return exe.cellTail.WithCell(f)
}

func (exe *Executor) Enqueue(fun func()) bool {
	return exe.send(applyQuery(fun))
}

func (exe *Executor) WithTerminatedChan(fun func(chan struct{})) {
	fun(exe.cellTail.Terminated)
}

func (exe *Executor) shutdown() {
	if exe.send(shutdownQuery{}) {
		exe.cellTail.Wait()
	}
}
