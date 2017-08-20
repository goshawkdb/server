package topologyTransmogrifier

import (
	"bytes"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/types/connectionmanager"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/types/topology"
	"goshawkdb.io/server/utils"
	"sync/atomic"
)

type emigrator struct {
	logger            log.Logger
	stop              int32
	self              common.RMId
	db                *db.Databases
	connectionManager connectionmanager.ConnectionManager
	activeBatches     map[common.RMId]*sendBatch
	topology          *configuration.Topology
	conns             map[common.RMId]*sconn.ServerConnection
}

func newEmigrator(task *migrate) *emigrator {
	e := &emigrator{
		logger:            task.inner.Logger,
		self:              task.self,
		db:                task.db,
		connectionManager: task.connectionManager,
		activeBatches:     make(map[common.RMId]*sendBatch),
	}
	e.topology = e.connectionManager.AddTopologySubscriber(topology.EmigratorSubscriber, e)
	e.connectionManager.AddServerConnectionSubscriber(e)
	return e
}

func (e *emigrator) stopAsync() {
	atomic.StoreInt32(&e.stop, 1)
	e.connectionManager.RemoveServerConnectionSubscriber(e)
	e.connectionManager.RemoveTopologySubscriberAsync(topology.EmigratorSubscriber, e)
}

func (e *emigrator) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	defer done(true)
	e.topology = topology
	e.startBatches()
}

func (e *emigrator) ConnectedRMs(conns map[common.RMId]*sconn.ServerConnection) {
	e.conns = conns
	e.startBatches()
}

func (e *emigrator) ConnectionLost(rmId common.RMId, conns map[common.RMId]*sconn.ServerConnection) {
	delete(e.activeBatches, rmId)
}

func (e *emigrator) ConnectionEstablished(conn *sconn.ServerConnection, conns map[common.RMId]*sconn.ServerConnection, done func()) {
	defer done()
	if conn.RMId == e.self {
		return
	}
	e.conns = conns
	e.startBatches()
}

func (e *emigrator) startBatches() {
	pending := e.topology.NextConfiguration.Pending
	batchConds := make([]*sendBatch, 0, len(pending))
	for rmId, cond := range pending {
		if rmId == e.self {
			continue
		}
		if _, found := e.activeBatches[rmId]; found {
			continue
		}
		if conn, found := e.conns[rmId]; found {
			e.logger.Log("msg", "Starting emigration batch.", "RMId", rmId)
			batch := e.newBatch(conn, cond.Cond)
			e.activeBatches[rmId] = batch
			batchConds = append(batchConds, batch)
		}
	}
	if len(batchConds) > 0 {
		e.startBatch(batchConds)
	}
}

func (e *emigrator) startBatch(batch []*sendBatch) {
	it := &dbIterator{
		emigrator:     e,
		configuration: e.topology.Configuration,
		batch:         batch,
	}
	go it.iterate()
}

type dbIterator struct {
	*emigrator
	configuration *configuration.Configuration
	batch         []*sendBatch
}

func (it *dbIterator) iterate() {
	ran, err := it.db.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		result, _ := rtxn.WithCursor(it.db.Vars, func(cursor *mdbs.Cursor) interface{} {
			vUUIdBytes, varBytes, err := cursor.Get(nil, nil, mdb.FIRST)
			for ; err == nil; vUUIdBytes, varBytes, err = cursor.Get(nil, nil, mdb.NEXT) {
				seg, _, err := capn.ReadFromMemoryZeroCopy(varBytes)
				if err != nil {
					cursor.Error(err)
					return true
				}
				varCap := msgs.ReadRootVar(seg)
				if bytes.Equal(varCap.Id(), configuration.TopologyVarUUId[:]) {
					continue
				}
				txnId := common.MakeTxnId(varCap.WriteTxnId())
				txnBytes := it.db.ReadTxnBytesFromDisk(cursor.RTxn, txnId)
				if txnBytes == nil {
					return true
				}
				txn := utils.TxnReaderFromData(txnBytes)
				// So, we only need to send based on the vars that we have
				// (in fact, we require the positions so we can only look
				// at the vars we have). However, the txn var allocations
				// only cover what's assigned to us at the time of txn
				// creation and that can change and we don't rewrite the
				// txn when it changes. So that all just means we must
				// ignore the allocations here, and just work through the
				// actions directly.
				actions := txn.Actions(true).Actions()
				varCaps, err := it.filterVars(cursor, vUUIdBytes, txnId[:], actions)
				if err != nil {
					return true
				} else if len(varCaps) == 0 {
					continue
				}
				for _, sb := range it.batch {
					matchingVarCaps, err := it.matchVarsAgainstCond(sb.cond, varCaps)
					if err != nil {
						cursor.Error(err)
						return true
					} else if len(matchingVarCaps) != 0 {
						sb.add(txn, matchingVarCaps)
					}
				}
			}
			if err == mdb.NotFound {
				return true
			} else {
				cursor.Error(err)
				return true
			}
		})
		return result
	}).ResultError()
	if err != nil {
		panic(fmt.Sprintf("Topology iterator error: %v", err))
	} else if ran != nil {
		for _, sb := range it.batch {
			sb.flush()
		}
		it.connectionManager.AddServerConnectionSubscriber(it)
	}
}

func (it *dbIterator) filterVars(cursor *mdbs.Cursor, vUUIdBytes []byte, txnIdBytes []byte, actions *msgs.Action_List) ([]*msgs.Var, error) {
	varCaps := make([]*msgs.Var, 0, actions.Len()>>1)
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		if action.Which() == msgs.ACTION_READ {
			// no point looking up the var itself as there's no way it'll
			// point back to us.
			continue
		}
		actionVarUUIdBytes := action.VarId()
		varBytes, err := cursor.RTxn.Get(it.db.Vars, actionVarUUIdBytes)
		if err == mdb.NotFound {
			continue
		} else if err != nil {
			cursor.Error(err)
			return nil, err
		}

		seg, _, err := capn.ReadFromMemoryZeroCopy(varBytes)
		if err != nil {
			cursor.Error(err)
			return nil, err
		}
		varCap := msgs.ReadRootVar(seg)
		if !bytes.Equal(txnIdBytes, varCap.WriteTxnId()) {
			// this var has moved on to a different txn
			continue
		}
		if bytes.Compare(actionVarUUIdBytes, vUUIdBytes) < 0 {
			// We've found an action on a var that is 'before' the
			// current var (will match ordering in lmdb) and it's on the
			// same txn as the current var. Therefore we've already done
			// this txn so we can just skip now.
			return nil, nil
		}
		varCaps = append(varCaps, &varCap)
	}
	return varCaps, nil
}

func (it *dbIterator) matchVarsAgainstCond(cond configuration.Cond, varCaps []*msgs.Var) ([]*msgs.Var, error) {
	result := make([]*msgs.Var, 0, len(varCaps)>>1)
	for _, varCap := range varCaps {
		pos := varCap.Positions()
		utils.DebugLog(it.logger, "debug", "Testing for condition.",
			"VarUUId", common.MakeVarUUId(varCap.Id()), "positions", (*common.Positions)(&pos),
			"condition", cond)
		if b, err := cond.SatisfiedBy(it.configuration, (*common.Positions)(&pos), it.logger); err == nil && b {
			result = append(result, varCap)
		} else if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (it *dbIterator) ConnectedRMs(conns map[common.RMId]*sconn.ServerConnection) {
	defer it.connectionManager.RemoveServerConnectionSubscriber(it)

	if atomic.LoadInt32(&it.stop) != 0 {
		return
	}

	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	mc := msgs.NewMigrationComplete(seg)
	mc.SetVersion(it.configuration.NextConfiguration.Version)
	msg.SetMigrationComplete(mc)
	bites := common.SegToBytes(seg)

	for _, sb := range it.batch {
		if conn, found := conns[sb.conn.RMId]; found && sb.conn == conn {
			// The connection has not changed since we started sending to
			// it (because we cached it, you can discount the issue of
			// memory reuse here - phew). Therefore, it's safe to send
			// the completion msg. If it has changed, we rely on the
			// ConnectionLost being called in the emigrator to do any
			// necessary tidying up.
			utils.DebugLog(it.logger, "debug", "Sending migration completion.", "recipient", conn.RMId)
			conn.Send(bites)
		}
	}
}
func (it *dbIterator) ConnectionLost(common.RMId, map[common.RMId]*sconn.ServerConnection) {}
func (it *dbIterator) ConnectionEstablished(conn *sconn.ServerConnection, servers map[common.RMId]*sconn.ServerConnection, done func()) {
	done()
}

type sendBatch struct {
	logger  log.Logger
	version uint32
	conn    *sconn.ServerConnection
	cond    configuration.Cond
	elems   []*migrationElem
}

type migrationElem struct {
	txn  *utils.TxnReader
	vars []*msgs.Var
}

func (e *emigrator) newBatch(conn *sconn.ServerConnection, cond configuration.Cond) *sendBatch {
	return &sendBatch{
		logger:  e.logger,
		version: e.topology.NextConfiguration.Version,
		conn:    conn,
		cond:    cond,
		elems:   make([]*migrationElem, 0, server.MigrationBatchElemCount),
	}
}

func (sb *sendBatch) flush() {
	if len(sb.elems) == 0 {
		return
	}
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	migration := msgs.NewMigration(seg)
	migration.SetVersion(sb.version)
	elems := msgs.NewMigrationElementList(seg, len(sb.elems))
	for idx, elem := range sb.elems {
		sb.elems[idx] = nil
		elemCap := msgs.NewMigrationElement(seg)
		elemCap.SetTxn(elem.txn.Data)
		vars := msgs.NewVarList(seg, len(elem.vars))
		for idy, varCap := range elem.vars {
			vars.Set(idy, *varCap)
		}
		elemCap.SetVars(vars)
		elems.Set(idx, elemCap)
	}
	migration.SetElems(elems)
	msg.SetMigration(migration)
	bites := common.SegToBytes(seg)
	utils.DebugLog(sb.logger, "debug", "Migrating txns.", "count", len(sb.elems), "recipient", sb.conn.RMId)
	sb.conn.Send(bites)
	sb.elems = sb.elems[:0]
}

func (sb *sendBatch) add(txn *utils.TxnReader, varCaps []*msgs.Var) {
	elem := &migrationElem{
		txn:  txn,
		vars: varCaps,
	}
	sb.elems = append(sb.elems, elem)
	if len(sb.elems) == server.MigrationBatchElemCount {
		sb.flush()
	}
}
