package paxos

import (
	"github.com/go-kit/kit/log"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server/db"
	eng "goshawkdb.io/server/txnengine"
	"goshawkdb.io/server/types/connectionmanager"
	"goshawkdb.io/server/types/localconnection"
)

type Dispatchers struct {
	db                 *db.Databases
	AcceptorDispatcher *AcceptorDispatcher
	VarDispatcher      *eng.VarDispatcher
	ProposerDispatcher *ProposerDispatcher
}

func NewDispatchers(cm connectionmanager.ConnectionManager, rmId common.RMId, bootCount uint32, count uint8, db *db.Databases, lc localconnection.LocalConnection, logger log.Logger) *Dispatchers {
	// All these dispatchers will synchronously create workers and do
	// loading. That loading will async call
	// cm.AddServerConnectionSubscriber and such like. This could lead
	// to local acceptors trying to message local proposers (eg sending
	// 2B/outcome messages) which is racy - AcceptorDispatcher is
	// created before ProposerDispatcher. To avoid this problem, the cm
	// starts off without the local node registered and only after this
	// call has completed is the cm told to register itself, but which
	// point we can be sure all the ProposerDispatchers/Managers exist.

	d := &Dispatchers{
		db:                 db,
		AcceptorDispatcher: NewAcceptorDispatcher(count, rmId, cm, db, logger),
		VarDispatcher:      eng.NewVarDispatcher(count, rmId, cm, db, lc, logger),
	}
	d.ProposerDispatcher = NewProposerDispatcher(count, rmId, bootCount, cm, db, d.VarDispatcher, logger)

	return d
}

func (d *Dispatchers) IsDatabaseEmpty() (bool, error) {
	res, err := d.db.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		res, _ := rtxn.WithCursor(d.db.Vars, func(cursor *mdbs.Cursor) interface{} {
			_, _, err := cursor.Get(nil, nil, mdb.FIRST)
			return err == mdb.NotFound
		})
		return res
	}).ResultError()
	if err != nil || res == nil {
		return false, err
	}
	return res.(bool), nil
}
