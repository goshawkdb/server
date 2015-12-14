package network

import (
	"bytes"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"log"
)

func GetTopologyFromLocalDatabase(cm *ConnectionManager, varDispatcher *eng.VarDispatcher, lc *client.LocalConnection) (*server.Topology, error) {
	if paxos.IsDatabaseClean(varDispatcher) {
		return nil, nil
	}

	for {
		seg := capn.NewBuffer(nil)
		txn := msgs.NewTxn(seg)
		txn.SetSubmitter(uint32(cm.RMId))
		txn.SetSubmitterBootCount(cm.BootCount)
		actions := msgs.NewActionList(seg, 1)
		txn.SetActions(actions)
		action := actions.At(0)
		action.SetVarId(server.TopologyVarUUId[:])
		action.SetRead()
		action.Read().SetVersion(common.VersionZero[:])
		allocs := msgs.NewAllocationList(seg, 1)
		txn.SetAllocations(allocs)
		alloc := allocs.At(0)
		alloc.SetRmId(uint32(cm.RMId))
		alloc.SetActive(cm.BootCount)
		indices := seg.NewUInt16List(1)
		alloc.SetActionIndices(indices)
		indices.Set(0, 0)
		txn.SetFInc(1)
		txn.SetTopologyVersion(0)

		result, err := lc.RunTransaction(&txn, true, cm.RMId)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, nil // shutting down
		}
		if result.Which() == msgs.OUTCOME_COMMIT {
			return nil, fmt.Errorf("Internal error: read of topology version 0 failed to abort")
		}
		abort := result.Abort()
		if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
			continue
		}
		abortUpdates := abort.Rerun()
		if abortUpdates.Len() != 1 {
			return nil, fmt.Errorf("Internal error: read of topology version 0 gave multiple updates")
		}
		update := abortUpdates.At(0)
		dbversion := common.MakeTxnId(update.TxnId())
		updateActions := update.Actions()
		if updateActions.Len() != 1 && updateActions.Len() != 2 {
			return nil, fmt.Errorf("Internal error: read of topology version 0 gave multiple actions: %v", updateActions.Len())
		}
		var updateAction *msgs.Action
		for idx, l := 0, updateActions.Len(); idx < l; idx++ {
			action := updateActions.At(idx)
			if bytes.Equal(action.VarId(), server.TopologyVarUUId[:]) {
				updateAction = &action
				break
			}
		}
		if updateAction == nil {
			return nil, fmt.Errorf("Internal error: unable to find action for topology from read of topology version 0")
		}
		if updateAction.Which() != msgs.ACTION_WRITE {
			return nil, fmt.Errorf("Internal error: read of topology version 0 gave non-write action")
		}
		write := updateAction.Write()
		var rootPtr *msgs.VarIdPos
		if refs := write.References(); refs.Len() == 1 {
			root := refs.At(0)
			rootPtr = &root
		}
		return server.TopologyDeserialize(dbversion, rootPtr, write.Value())
	}
}

func CreateTopologyZero(cm *ConnectionManager, topology *server.Topology, lc *client.LocalConnection) (*common.TxnId, error) {
	seg := capn.NewBuffer(nil)
	txn := msgs.NewTxn(seg)
	txnId := server.VersionOne
	txn.SetId(txnId[:])
	txn.SetSubmitter(uint32(cm.RMId))
	txn.SetSubmitterBootCount(cm.BootCount)
	actions := msgs.NewActionList(seg, 1)
	txn.SetActions(actions)
	action := actions.At(0)
	action.SetVarId(server.TopologyVarUUId[:])
	action.SetCreate()
	create := action.Create()
	positions := seg.NewUInt8List(int(topology.MaxRMCount))
	create.SetPositions(positions)
	for idx, l := 0, positions.Len(); idx < l; idx++ {
		positions.Set(idx, uint8(idx))
	}
	create.SetValue(topology.Serialize())
	create.SetReferences(msgs.NewVarIdPosList(seg, 0))
	allocs := msgs.NewAllocationList(seg, 1)
	txn.SetAllocations(allocs)
	alloc := allocs.At(0)
	alloc.SetRmId(uint32(cm.RMId))
	alloc.SetActive(cm.BootCount)
	indices := seg.NewUInt16List(1)
	alloc.SetActionIndices(indices)
	indices.Set(0, 0)
	txn.SetFInc(1)
	txn.SetTopologyVersion(topology.Version)
	result, err := lc.RunTransaction(&txn, false, cm.RMId)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil // shutting down
	}
	if result.Which() == msgs.OUTCOME_COMMIT {
		return txnId, nil
	} else {
		return nil, fmt.Errorf("Internal error: unable to write initial topology to local data store")
	}
}

func AddSelfToTopology(cm *ConnectionManager, conns map[common.RMId]paxos.Connection, topology *server.Topology, fInc int, active, passive []common.RMId, lc *client.LocalConnection) (*server.Topology, bool, error) {
	seg := capn.NewBuffer(nil)
	txn := msgs.NewTxn(seg)
	txn.SetSubmitter(uint32(cm.RMId))
	txn.SetSubmitterBootCount(cm.BootCount)

	actions := msgs.NewActionList(seg, 1)
	txn.SetActions(actions)
	topologyAction := actions.At(0)
	topologyAction.SetVarId(server.TopologyVarUUId[:])
	topologyAction.SetReadwrite()
	rw := topologyAction.Readwrite()
	rw.SetVersion(topology.DBVersion[:])
	rw.SetValue(topology.Serialize())
	if topology.RootVarUUId == nil {
		rw.SetReferences(msgs.NewVarIdPosList(seg, 0))
	} else {
		refs := msgs.NewVarIdPosList(seg, 1)
		rw.SetReferences(refs)
		varIdPos := refs.At(0)
		varIdPos.SetId(topology.RootVarUUId[:])
		varIdPos.SetPositions((capn.UInt8List)(*topology.RootPositions))
	}

	allocs := msgs.NewAllocationList(seg, len(topology.AllRMs))
	txn.SetAllocations(allocs)
	idx := 0
	for listIdx, rmIds := range [][]common.RMId{active, passive} {
		for _, rmId := range rmIds {
			alloc := allocs.At(idx)
			idx++
			alloc.SetRmId(uint32(rmId))
			if listIdx == 0 {
				alloc.SetActive(conns[rmId].BootCount())
			} else {
				alloc.SetActive(0)
			}
			indices := seg.NewUInt16List(1)
			alloc.SetActionIndices(indices)
			indices.Set(0, 0)
		}
	}
	txn.SetFInc(uint8(fInc))
	txn.SetTopologyVersion(topology.Version)

	result, err := lc.RunTransaction(&txn, true, active...)
	if err != nil || result == nil {
		return nil, false, err
	}
	txnId := common.MakeTxnId(result.Txn().Id())
	if result.Which() == msgs.OUTCOME_COMMIT {
		topology.DBVersion = txnId
		server.Log("Topology Txn Committed ok with txnId", topology.DBVersion)
		return topology, false, nil
	}
	abort := result.Abort()
	server.Log("Topology Txn Aborted", txnId)
	if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
		return nil, true, nil
	}
	abortUpdates := abort.Rerun()
	if abortUpdates.Len() != 1 {
		return nil, false, fmt.Errorf("Internal error: readwrite of topology gave %v updates (1 expected)", abortUpdates.Len())
	}
	update := abortUpdates.At(0)
	dbversion := common.MakeTxnId(update.TxnId())

	updateActions := update.Actions()
	if updateActions.Len() != 1 && updateActions.Len() != 2 {
		return nil, false, fmt.Errorf("Internal error: readwrite of topology gave multiple actions: %v", updateActions.Len())
	}
	var updateAction *msgs.Action
	for idx, l := 0, updateActions.Len(); idx < l; idx++ {
		action := updateActions.At(idx)
		if bytes.Equal(action.VarId(), server.TopologyVarUUId[:]) {
			updateAction = &action
			break
		}
	}

	if updateAction == nil {
		return nil, false, fmt.Errorf("Internal error: unable to find action for topology from readwrite")
	}
	if updateAction.Which() != msgs.ACTION_WRITE {
		return nil, false, fmt.Errorf("Internal error: readwrite of topology gave non-write action")
	}
	write := updateAction.Write()
	var rootVarPos *msgs.VarIdPos
	if refs := write.References(); refs.Len() == 1 {
		root := refs.At(0)
		rootVarPos = &root
	} else if refs.Len() > 1 {
		return nil, false, fmt.Errorf("Internal error: readwrite of topology had wrong references: %v", refs.Len())
	}
	topology, err = server.TopologyDeserialize(dbversion, rootVarPos, write.Value())
	if err != nil {
		return nil, false, err
	}
	found := false
	for _, rmId := range topology.AllRMs {
		if found = rmId == cm.RMId; found {
			server.Log("Topology Txn Aborted, but found self in topology.")
			return topology, false, nil
		}
	}
	return topology, true, nil
}

func MaybeCreateRoot(topology *server.Topology, conns map[common.RMId]paxos.Connection, cm *ConnectionManager, lc *client.LocalConnection) error {
	if topology.RootVarUUId != nil {
		return nil
	}
	requiredKnownRMs := int(topology.TwoFInc)
	if len(topology.AllRMs) < requiredKnownRMs {
		return nil
	}
	activeRMs := make([]common.RMId, 0, int(topology.FInc))
	passiveRMs := make([]common.RMId, 0, int(topology.F))
	for _, rmId := range topology.AllRMs[:requiredKnownRMs] {
		if _, found := conns[rmId]; found && len(activeRMs) < cap(activeRMs) {
			activeRMs = append(activeRMs, rmId)
		} else {
			passiveRMs = append(passiveRMs, rmId)
		}
	}
	if len(activeRMs) < cap(activeRMs) {
		return nil
	}

	server.Log("Creating Root. Actives:", activeRMs, "; Passives:", passiveRMs)
	for {
		seg := capn.NewBuffer(nil)
		txn := msgs.NewTxn(seg)
		txn.SetSubmitter(uint32(cm.RMId))
		txn.SetSubmitterBootCount(cm.BootCount)
		actions := msgs.NewActionList(seg, 1)
		txn.SetActions(actions)
		action := actions.At(0)
		vUUId := lc.NextVarUUId()
		action.SetVarId(vUUId[:])
		action.SetCreate()
		create := action.Create()
		positions := seg.NewUInt8List(int(topology.MaxRMCount))
		create.SetPositions(positions)
		for idx, l := 0, positions.Len(); idx < l; idx++ {
			positions.Set(idx, uint8(idx))
		}
		create.SetValue([]byte{})
		create.SetReferences(msgs.NewVarIdPosList(seg, 0))
		allocs := msgs.NewAllocationList(seg, requiredKnownRMs)
		txn.SetAllocations(allocs)
		idx := 0
		for listIdx, rmIds := range [][]common.RMId{activeRMs, passiveRMs} {
			for _, rmId := range rmIds {
				alloc := allocs.At(idx)
				idx++
				alloc.SetRmId(uint32(rmId))
				if listIdx == 0 {
					alloc.SetActive(conns[rmId].BootCount())
				} else {
					alloc.SetActive(0)
				}
				indices := seg.NewUInt16List(1)
				alloc.SetActionIndices(indices)
				indices.Set(0, 0)
			}
		}
		txn.SetFInc(topology.FInc)
		txn.SetTopologyVersion(topology.Version)
		result, err := lc.RunTransaction(&txn, true, activeRMs...)
		if err != nil {
			return err
		}
		if result == nil {
			return nil
		}
		if result.Which() == msgs.OUTCOME_COMMIT {
			server.Log("Root created in", vUUId)
			topology.RootVarUUId = vUUId
			topology.RootPositions = (*common.Positions)(&positions)
			return nil
		}
		abort := result.Abort()
		if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
			continue
		}
		return fmt.Errorf("Internal error: creation of root gave rerun outcome")
	}
}

type TopologyWriter struct {
	toWrite           *server.Topology
	localConnection   *client.LocalConnection
	connectionManager *ConnectionManager
	finished          bool
}

func NewTopologyWriter(topology *server.Topology, lc *client.LocalConnection, cm *ConnectionManager) *TopologyWriter {
	return &TopologyWriter{
		toWrite:           topology.Clone(),
		localConnection:   lc,
		connectionManager: cm,
		finished:          false,
	}
}

func (tw *TopologyWriter) ConnectedRMs(conns map[common.RMId]paxos.Connection) {
	tw.maybeStartWrite(conns)
}

func (tw *TopologyWriter) ConnectionLost(rmId common.RMId, conns map[common.RMId]paxos.Connection) {
}

func (tw *TopologyWriter) ConnectionEstablished(rmId common.RMId, conn paxos.Connection, conns map[common.RMId]paxos.Connection) {
	tw.maybeStartWrite(conns)
}

func (tw *TopologyWriter) maybeStartWrite(conns map[common.RMId]paxos.Connection) {
	if tw.finished {
		return
	}
	var (
		activeRMs  common.RMIds
		passiveRMs common.RMIds
	)
	toWrite := tw.toWrite
	fInc := (len(toWrite.Hosts) >> 1) + 1
	if remoteTopology := tw.connectionManager.remoteTopology; remoteTopology == nil {
		if len(conns) < fInc {
			return
		}
		rmIds := make([]common.RMId, 0, len(conns))
		for rmId := range conns {
			rmIds = append(rmIds, rmId)
		}
		toWrite.AllRMs = rmIds
		activeRMs = rmIds

	} else {
		toWrite = remoteTopology.Clone()
		fInc = (len(toWrite.Hosts) >> 1) + 1
		foundSelf := false
		for _, rmId := range toWrite.AllRMs {
			if foundSelf = rmId == tw.connectionManager.RMId; foundSelf {
				break
			}
		}
		if !foundSelf {
			toWrite.AllRMs = append(toWrite.AllRMs, tw.connectionManager.RMId)
		}

		activeRMs = make([]common.RMId, 0, fInc)
		passiveRMs = make([]common.RMId, 0, len(toWrite.AllRMs)-fInc)
		if foundSelf {
			activeRMs = append(activeRMs, tw.connectionManager.RMId)
		} else {
			passiveRMs = append(passiveRMs, tw.connectionManager.RMId)
		}
		for _, rmId := range toWrite.AllRMs {
			if rmId == tw.connectionManager.RMId {
				continue
			}
			if _, found := conns[rmId]; found && len(activeRMs) < cap(activeRMs) {
				activeRMs = append(activeRMs, rmId)
			} else {
				passiveRMs = append(passiveRMs, rmId)
			}
		}
		if len(activeRMs) < cap(activeRMs) {
			return
		}
	}

	tw.finished = true
	tw.connectionManager.RemoveSenderAsync(tw)
	server.Log("Topology Txn: Active:", activeRMs, "; Passive:", passiveRMs, "; ToWrite:", toWrite)
	go func() { // we're in connectionManager's go-routine here. Don't block it with the following!
		if err := MaybeCreateRoot(toWrite, conns, tw.connectionManager, tw.localConnection); err != nil {
			log.Println("Error when creating root:", err)
			return
		}
		tw.toWrite.RootVarUUId = toWrite.RootVarUUId
		tw.toWrite.RootPositions = toWrite.RootPositions
		topology, restart, err := AddSelfToTopology(tw.connectionManager, conns, toWrite, fInc, activeRMs, passiveRMs, tw.localConnection)
		if restart {
			if topology == nil {
				topology = tw.toWrite
			} else if topology.RootVarUUId == nil {
				topology.RootVarUUId = toWrite.RootVarUUId
				topology.RootPositions = toWrite.RootPositions
			}
			tw.connectionManager.AddSender(NewTopologyWriter(topology, tw.localConnection, tw.connectionManager))
		} else if err != nil {
			log.Println("Error when adding self to topology:", err)
			return
		}
	}()
}
