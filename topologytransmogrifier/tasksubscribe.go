package topologytransmogrifier

import (
	"bytes"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/utils/binarybackoff"
	"goshawkdb.io/server/utils/txnreader"
	"time"
)

type subscribe struct {
	*transmogrificationTask
	subscribed bool
}

func (task *subscribe) init(base *transmogrificationTask) {
	task.transmogrificationTask = base
	task.subscribed = false
}

func (task *subscribe) isValid() bool {
	active := task.activeTopology
	return active != nil && len(active.ClusterId) > 0 &&
		task.targetConfig != nil &&
		active.NextConfiguration != nil &&
		active.NextConfiguration.Version == task.targetConfig.Version &&
		!task.subscribed
}

func (task *subscribe) announce() {
	task.inner.Logger.Log("stage", "Subscribe", "msg", "Subscribing to topology.", "configuration", task.targetConfig)
}

func (task *subscribe) Tick() (bool, error) {
	if task.selectStage() != task {
		return task.completed()
	}

	next := task.activeTopology.NextConfiguration
	localHost, err := task.firstLocalHost(task.activeTopology.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	remoteHosts := task.allHostsBarLocalHost(localHost, next)
	task.installTopology(task.activeTopology, nil, localHost, remoteHosts)
	task.ensureShareGoalWithAll()

	// From this point onwards, we have the possibility that some
	// node-to-be-removed has rushed ahead and has shutdown. So we
	// can't rely on any to-be-removed node. So that means we can only
	// rely on the nodes in next.RMs, which means we need a majority of
	// them to be alive; and we use the removed RMs as extra passives.
	active, passive := task.formActivePassive(next.RMs, next.LostRMIds)
	if active == nil {
		return false, nil
	}

	task.inner.Logger.Log("msg", "Subscribing to topology.",
		"active", fmt.Sprint(active), "passive", fmt.Sprint(passive))

	topology := task.activeTopology.Clone()

	seg := capn.NewBuffer(nil)
	txn := msgs.NewRootTxn(seg)

	actionsSeg := capn.NewBuffer(nil)
	actionsWrapper := msgs.NewRootActionListWrapper(actionsSeg)
	actions := msgs.NewActionList(actionsSeg, 1)
	actionsWrapper.SetActions(actions)
	action := actions.At(0)
	action.SetVarId(configuration.TopologyVarUUId[:])
	action.SetVersion(topology.DBVersion[:])
	action.SetUnmodified()
	action.SetActionType(msgs.ACTIONTYPE_ADDSUBSCRIPTION)
	txn.SetActions(common.SegToBytes(actionsSeg))

	allocs := msgs.NewAllocationList(seg, len(active)+len(passive))
	txn.SetAllocations(allocs)

	offset := 0
	for idx, rmIds := range []common.RMIds{active, passive} {
		for idy, rmId := range rmIds {
			alloc := allocs.At(idy + offset)
			alloc.SetRmId(uint32(rmId))
			if idx == 0 {
				alloc.SetActive(task.activeConnections[rmId].BootCount)
			} else {
				alloc.SetActive(0)
			}
			indices := seg.NewUInt16List(1)
			alloc.SetActionIndices(indices)
			indices.Set(0, 0)
		}
		offset += len(rmIds)
	}

	twoFInc := uint16(next.RMs.NonEmptyLen())
	txn.SetTwoFInc(twoFInc)
	txn.SetTopologyVersion(topology.Version)
	txn.SetIsTopology(true)

	task.runTxnMsg = &topologyTransmogrifierMsgAddSubscription{
		transmogrificationTask: task.transmogrificationTask,
		task:                 task,
		backoff:              binarybackoff.NewBinaryBackoffEngine(task.rng, 2*time.Second, time.Duration(len(task.targetConfig.Hosts)+1)*2*time.Second),
		txn:                  &txn,
		subscriptionConsumer: task.SubscriptionConsumer,
		target:               topology,
		active:               active,
		passive:              passive,
	}
	return task.runTxnMsg.Exec()
}

func (task *subscribe) SubscriptionConsumer(txn *txnreader.TxnReader, tr *client.TransactionRecord) error {
	actions := txn.Actions(true).Actions()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		if bytes.Compare(action.VarId(), configuration.TopologyVarUUId[:]) != 0 {
			continue
		}

		// We only get passed actions which contain the real value, so
		// we don't need to check actionType.
		mod := action.Modified()
		value := mod.Value()
		refs := mod.References()
		if topology, err := configuration.TopologyFromCap(txn.Id, &refs, value); err != nil {
			return err
		} else {
			task.EnqueueMsg(topologyTransmogrifierMsgTopologyObserved{
				TopologyTransmogrifier: task.TopologyTransmogrifier,
				topology:               topology,
			})
		}
		return nil
	}
	return nil
}
