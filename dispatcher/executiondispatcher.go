package dispatcher

import (
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common/actor"
)

type Dispatcher struct {
	ExecutorCount uint8
	Executors     []*Executor
}

func (dis *Dispatcher) Init(count uint8, logger log.Logger) {
	executors := make([]*Executor, count)
	for idx := range executors {
		executors[idx] = newExecutor(log.With(logger, "instance", idx))
	}
	dis.Executors = executors
	dis.ExecutorCount = count
}

func (dis *Dispatcher) ShutdownSync() {
	for _, exe := range dis.Executors {
		exe.ShutdownSync()
	}
}

type Executor struct {
	*actor.BasicServerOuter
	*actor.Mailbox
	log.Logger
}

func newExecutor(logger log.Logger) *Executor {
	inner := actor.NewBasicServerInner(logger)
	mailbox, err := actor.Spawn(inner)
	if err != nil {
		panic(err) // "impossible"
	}

	exe := &Executor{
		BasicServerOuter: actor.NewBasicServerOuter(mailbox),
		Mailbox:          mailbox,
		Logger:           logger,
	}
	return exe
}
