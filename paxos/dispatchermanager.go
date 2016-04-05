package paxos

import (
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server/db"
	eng "goshawkdb.io/server/txnengine"
)

type Dispatchers struct {
	db                 *db.Databases
	AcceptorDispatcher *AcceptorDispatcher
	VarDispatcher      *eng.VarDispatcher
	ProposerDispatcher *ProposerDispatcher
	connectionManager  ConnectionManager
}

func NewDispatchers(cm ConnectionManager, rmId common.RMId, count uint8, db *db.Databases, lc eng.LocalConnection) *Dispatchers {
	// It actually doesn't matter at this point what order we start up
	// the acceptors. This is because we are called from the
	// ConnectionManager constructor, and its actor loop hasn't been
	// started at this point. Thus whilst AddSender msgs can be sent,
	// none will be processed until after we return from this. So an
	// acceptor sending 2B msgs to a proposer will not get sent until
	// after all the proposers have been loaded off disk.

	d := &Dispatchers{
		db:                 db,
		AcceptorDispatcher: NewAcceptorDispatcher(rmId, cm, count, db),
		VarDispatcher:      eng.NewVarDispatcher(count, db, lc),
		connectionManager:  cm,
	}
	d.ProposerDispatcher = NewProposerDispatcher(count, rmId, d.VarDispatcher, cm, db)

	return d
}

func (d *Dispatchers) IsDatabaseEmpty() (bool, error) {
	res, err := d.db.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		res, _ := rtxn.WithCursor(d.db.Transactions, func(cursor *mdbs.Cursor) interface{} {
			_, _, err := cursor.Get(nil, nil, mdb.FIRST)
			return err == mdb.NotFound
		})
		return res
	}).ResultError()
	if err != nil {
		return false, err
	}
	return res.(bool), nil
}
