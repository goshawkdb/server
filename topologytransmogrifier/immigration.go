package topologyTransmogrifier

import (
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	eng "goshawkdb.io/server/txnengine"
	"sync/atomic"
)

type topologyTransmogrifierMsgImmigrationReceived struct {
	*TopologyTransmogrifier
	migration *msgs.Migration
	sender    common.RMId
}

func (msg topologyTransmogrifierMsgImmigrationReceived) Exec() (bool, error) {
	version := msg.migration.Version()
	if version <= msg.active.Version {
		// This topology change has been completed. Ignore this migration.
		return false, nil
	} else if next := msg.active.NextConfiguration; next != nil {
		if version < next.Version {
			// Whatever change that was for, it isn't happening any
			// more. Ignore.
			return false, nil
		} else if _, found := next.Pending[msg.self]; version == next.Version && !found {
			// Migration is for the current topology change, but we've
			// declared ourselves done, so Ignore.
			return false, nil
		}
	}

	senders, found := msg.migrations[version]
	if !found {
		senders = make(map[common.RMId]*int32)
		msg.migrations[version] = senders
	}
	sender := msg.sender
	inprogressPtr, found := senders[sender]
	if found {
		atomic.AddInt32(inprogressPtr, 1)
	} else {
		inprogress := int32(2)
		inprogressPtr = &inprogress
		senders[sender] = inprogressPtr
	}
	txnCount := int32(msg.migration.Elems().Len())
	lsc := msg.newTxnLSC(txnCount, inprogressPtr)
	msg.connectionManager.Dispatchers.ProposerDispatcher.ImmigrationReceived(msg.migration, lsc)
	return false, nil
}

func (tt *TopologyTransmogrifier) ImmigrationReceived(sender common.RMId, migration *msgs.Migration) {
	tt.EnqueueMsg(topologyTransmogrifierMsgMigrationReceived{
		TopologyTransmogrifier: tt,
		migration:              migration,
		sender:                 sender,
	})
}

type topologyTransmogrifierMsgImmigrationComplete struct {
	*TopologyTransmogrifier
	complete *msgs.MigrationComplete
	sender   common.RMId
}

func (msg topologyTransmogrifierMsgImmigrationComplete) Exec() (bool, error) {
	version := msg.complete.Version()
	sender := msg.sender
	server.DebugLog(msg.inner.Logger, "debug", "MCR received.", "sender", sender, "version", version)
	senders, found := msg.migrations[version]
	if !found {
		if version > msg.active.Version {
			senders = make(map[common.RMId]*int32)
			msg.migrations[version] = senders
		} else {
			return false, nil
		}
	}
	inprogress := int32(0)
	if inprogressPtr, found := senders[sender]; found {
		inprogress = atomic.AddInt32(inprogressPtr, -1)
	} else {
		inprogressPtr = &inprogress
		senders[sender] = inprogressPtr
	}
	// race here?!
	if inprogress == 0 {
		return msg.maybeTick()
	}
	return false, nil
}

func (tt *TopologyTransmogrifier) ImmigrationCompleteReceived(sender common.RMId, migrationComplete *msgs.MigrationComplete) {
	tt.EnqueueMsg(topologyTransmogrifierMsgImmigrationComplete{
		TopologyTransmogrifier: tt,
		complete:               migrationComplete,
		sender:                 sender,
	})
}

func (tt *TopologyTransmogrifier) newTxnLSC(txnCount int32, inprogressPtr *int32) eng.TxnLocalStateChange {
	return &immigrationTxnLocalStateChange{
		TopologyTransmogrifier: tt,
		pendingLocallyComplete: txnCount,
		inprogressPtr:          inprogressPtr,
	}
}

type immigrationTxnLocalStateChange struct {
	*TopologyTransmogrifier
	pendingLocallyComplete int32
	inprogressPtr          *int32
}

func (mtlsc *immigrationTxnLocalStateChange) TxnBallotsComplete(...*eng.Ballot) {
	panic("TxnBallotsComplete called on migrating txn.")
}

// Careful: we're in the proposer dispatcher go routine here!
func (mtlsc *immigrationTxnLocalStateChange) TxnLocallyComplete(txn *eng.Txn) {
	txn.CompletionReceived()
	if atomic.AddInt32(&mtlsc.pendingLocallyComplete, -1) == 0 &&
		atomic.AddInt32(mtlsc.inprogressPtr, -1) == 0 {
		mtlsc.EnqueueFuncAsync(func() (bool, error) {
			if mtlsc.currentTask != nil {
				return mtlsc.currentTask.Tick()
			}
			return false, nil
		})
	}
}

func (mtlsc *migrationTxnLocalStateChange) TxnFinished(*eng.Txn) {}
