package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/types"
	"goshawkdb.io/server/types/actor"
	cconn "goshawkdb.io/server/types/connections/client"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/binarybackoff"
	ch "goshawkdb.io/server/utils/consistenthash"
	"goshawkdb.io/server/utils/txnreader"
	"goshawkdb.io/server/utils/vectorclock"
	"math/rand"
	"time"
)

// We get client txns from two different sources.
//
// 1. LocalConnection (used by frame for rolls, and by transmogrifier
// for createRoots). See local.go. These don't really get validated -
// we just allow them to provide additional position maps, and the
// code basically just assumes full ReadWrite capabilities
// everywhere. Essentially, the "connection" is not stateful, so the
// clients of LocalConnection must provide whatever state is necessary
// to get the transaction to be translated. These txns do not get
// auto-resubmitted - instead it's the responsibility of the caller to
// LocalConnection to do any resubmission and rerunning.
//
// 2. Transactions from real clients. Here, the connection really is
// stateful and validation is meaningful. If the transaction aborts
// with resubmit, then that happens. If it aborts with rerun then we
// must figure out what changes need sending back down to the client
// and do that work too.

type RemoteTransactionSubmitter struct {
	*TransactionSubmitter
	minTxnCount    uint64
	namespace      []byte
	cache          *Cache
	bbe            *binarybackoff.BinaryBackoffEngine
	metrics        *cconn.ClientTxnMetrics
	cont           RemoteTxnCompletionContinuation
	resubmitCount  int
	pendingUpdates []func() error
}

func NewRemoteTransactionSubmitter(namespace []byte, connPub sconn.ServerConnectionPublisher, actor actor.EnqueueActor, rng *rand.Rand, logger log.Logger, roots map[common.VarUUId]*types.PosCapVer, metrics *cconn.ClientTxnMetrics) *RemoteTransactionSubmitter {
	return &RemoteTransactionSubmitter{
		TransactionSubmitter: NewTransactionSubmitter(connPub, actor, rng, logger),
		minTxnCount:          0,
		namespace:            namespace,
		cache:                NewCache(rng, roots),
		bbe:                  binarybackoff.NewBinaryBackoffEngine(rng, server.SubmissionMinSubmitDelay, server.SubmissionMaxSubmitDelay),
		metrics:              metrics,
	}
}

func (rts *RemoteTransactionSubmitter) TopologyChanged(topology *configuration.Topology) error {
	utils.DebugLog(rts.logger, "debug", "RTS Topology Changed.", "topology", topology, "blank", topology.IsBlank())
	if !topology.IsBlank() {
		rts.cache.SetResolver(ch.NewResolver(topology.RMs, topology.TwoFInc))
	}
	return rts.TransactionSubmitter.TopologyChanged(topology)
}

type RemoteTxnCompletionContinuation func(*cmsgs.ClientTxnOutcome, error) error

func (rts *RemoteTransactionSubmitter) Committed(txn *txnreader.TxnReader, tr *TransactionRecord) error {
	cont := rts.cont
	rts.cont = nil

	if rts.metrics != nil {
		rts.metrics.TxnLatency.Observe(float64(int64(time.Now().Sub(tr.birthday))) / float64(time.Second))
		rts.metrics.TxnResubmit.Add(float64(rts.resubmitCount))
	}

	utils.DebugLog(rts.logger, "debug", "Txn Committed.", "OrigTxnId", tr.origId, "TxnId", tr.Id)

	rts.bbe.Shrink(server.SubmissionMinSubmitDelay)

	rts.minTxnCount = binary.BigEndian.Uint64(txn.Id[:8]) + 1

	seg := capn.NewBuffer(nil)
	clientOutcome := cmsgs.NewClientTxnOutcome(seg)
	clientOutcome.SetId(tr.origId[:])
	clientOutcome.SetFinalId(txn.Id[:])
	clientOutcome.SetCounter(0)
	clientOutcome.SetCommit()

	clock := vectorclock.VectorClockFromData(tr.outcome.Commit(), false)
	actions := txn.Actions(true).Actions()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		if !txnreader.IsWrite(&action) {
			continue
		}
		vUUId := common.MakeVarUUId(action.VarId())
		c, found := tr.objs[*vUUId]
		if !found {
			panic("Failed to find object in transactionRecord cache! " + vUUId.String())
		}
		if c.caps.CanRead() {
			c.version = txn.Id
			c.clockElem = clock.At(vUUId)
		}
		// the txn cannot have widenend any capabilities, so we do not
		// need to iterate through c.refs at all.
		value := action.Value()
		valueWhich := value.Which()
		if valueWhich == msgs.ACTIONVALUE_CREATE {
			rts.cache.AddCached(vUUId, c)
			c.refs = value.Create().References().ToArray()
		} else if valueWhich == msgs.ACTIONVALUE_EXISTING {
			modify := value.Existing().Modify()
			if modify.Which() == msgs.ACTIONVALUEEXISTINGMODIFY_WRITE {
				c.refs = modify.Write().References().ToArray()
			}
		}
	}

	if err := cont(&clientOutcome, nil); err != nil {
		return err
	} else {
		return rts.processPendingUpdates()
	}
}

func (rts *RemoteTransactionSubmitter) Aborted(txn *txnreader.TxnReader, tr *TransactionRecord) error {
	cont := rts.cont
	rts.cont = nil

	txnId := txn.Id

	abort := tr.outcome.Abort()
	resubmit := abort.Which() == msgs.OUTCOMEABORT_RESUBMIT

	if !resubmit {
		updates := abort.Rerun()
		validUpdates := rts.filterUpdates(&updates, tr)
		utils.DebugLog(rts.logger, "debug", "Txn Outcome.", "TxnId", txnId,
			"updatesLen", updates.Len(), "validLen", len(validUpdates))

		if len(validUpdates) != 0 {
			rts.bbe.Shrink(server.SubmissionMinSubmitDelay)
			rts.minTxnCount = binary.BigEndian.Uint64(txn.Id[:8]) + 1
			// we actually have to get the client to rerun
			clientSeg := capn.NewBuffer(nil)
			clientOutcome := cmsgs.NewClientTxnOutcome(clientSeg)
			clientOutcome.SetId(tr.origId[:])
			clientOutcome.SetFinalId(txn.Id[:])
			clientOutcome.SetCounter(0)

			clientActions := cmsgs.NewClientActionList(clientSeg, len(validUpdates))
			idx := 0
			for vUUId, vc := range validUpdates {
				clientAction := clientActions.At(idx)
				idx++
				clientAction.SetVarId(vUUId[:])
				clientValue := clientAction.Value()
				if vc.val == nil {
					utils.DebugLog(rts.logger, "debug", "TxnId", txnId, "VarUUId", vUUId, "update", "DELETE")
					clientValue.SetMissing()
				} else {
					utils.DebugLog(rts.logger, "debug", "TxnId", txnId, "VarUUId", vUUId, "update", "WRITEONLY")
					clientValue.SetExisting()
					clientModify := clientValue.Existing().Modify()
					clientModify.SetWrite()
					clientWrite := clientModify.Write()
					clientWrite.SetValue(vc.val)
					clientRefs := cmsgs.NewClientVarIdPosList(clientSeg, len(vc.c.refs))
					for idy, ref := range vc.c.refs {
						clientRef := clientRefs.At(idy)
						clientRef.SetVarId(ref.Id())
						clientRef.SetCapability(ref.Capability())
					}
					clientWrite.SetReferences(clientRefs)
				}
			}
			clientOutcome.SetAbort(clientActions)

			if rts.metrics != nil {
				rts.metrics.TxnRerun.Inc()
				rts.metrics.TxnResubmit.Add(float64(rts.resubmitCount))
			}
			if err := cont(&clientOutcome, nil); err != nil {
				return err
			} else {
				return rts.processPendingUpdates()
			}
		}
	}

	utils.DebugLog(rts.logger, "debug", "Resubmitting Txn.", "TxnId", txn.Id,
		"origResubmit", abort.Which() == msgs.OUTCOMEABORT_RESUBMIT, "OrigTxnId", tr.origId)
	rts.resubmitCount++
	rts.bbe.Advance()

	curTxnIdNum := binary.BigEndian.Uint64(txnId[:8])
	curTxnIdNum += 1 + uint64(rts.rng.Intn(8))
	binary.BigEndian.PutUint64(txnId[:8], curTxnIdNum)

	// We choose to retranslate the client txn for two reasons:
	// 1. The topology could have changed;
	// 2. Which RMs are up or down could have changed;
	// In either case, we will need to recalculate which RMs have which objects.

	clientSeg := capn.NewBuffer(nil)
	client := cmsgs.NewClientTxn(clientSeg)
	client.SetId(txnId[:])
	client.SetCounter(tr.client.Counter())
	client.SetActions(tr.client.Actions())

	return rts.submitRemoteClientTransaction(tr.origId, txnId, &client, cont, false)
}

func (rts *RemoteTransactionSubmitter) SubmitRemoteClientTransaction(txnId *common.TxnId, txn *cmsgs.ClientTxn, cont RemoteTxnCompletionContinuation) error {
	return rts.submitRemoteClientTransaction(txnId, txnId, txn, cont, false)
}

func (rts *RemoteTransactionSubmitter) submitRemoteClientTransaction(origTxnId, txnId *common.TxnId, txn *cmsgs.ClientTxn, cont RemoteTxnCompletionContinuation, forceSubmission bool) error {
	if rts.cont != nil {
		return cont(nil, errors.New("Live Transaction already exists."))
	} else if !bytes.Equal(txnId[8:], rts.namespace) || binary.BigEndian.Uint64(txnId[:8]) < rts.minTxnCount {
		return cont(nil, fmt.Errorf("Illegal txnId %v", txnId))
	}

	if rts.topology.IsBlank() {
		rts.bufferedSubmissions = append(rts.bufferedSubmissions, func() {
			rts.submitRemoteClientTransaction(origTxnId, txnId, txn, cont, forceSubmission)
		})
		return nil

	} else {
		tr := &TransactionRecord{
			TransactionSubmitter:       rts.TransactionSubmitter,
			transactionOutcomeReceiver: rts,
			cache:  rts.cache,
			Id:     txnId,
			origId: origTxnId,
			client: txn,
			bbe:    rts.bbe,
		}
		if addsSubs, err := tr.formServerTxn(rts.validateCreatesCallback, false); err == badCounter {
			clientSeg := capn.NewBuffer(nil)
			clientOutcome := cmsgs.NewClientTxnOutcome(clientSeg)
			clientOutcome.SetId(origTxnId[:])
			clientOutcome.SetFinalId(txnId[:])
			clientOutcome.SetCounter(0)
			clientActions := cmsgs.NewClientActionList(clientSeg, 0)
			clientOutcome.SetAbort(clientActions)
			return cont(&clientOutcome, nil)

		} else if err != nil {
			return cont(nil, err)

		} else if addsSubs {
			tr.subManager = NewSubscriptionManager(txnId, tr, rts.SubscriptionConsumer)
		}

		rts.cont = cont
		rts.resubmitCount = 0
		rts.pendingUpdates = []func() error{}
		rts.AddTransactionRecord(tr, forceSubmission)
		tr.Submit()
		if rts.metrics != nil {
			rts.metrics.TxnSubmit.Inc()
		}
		return nil
	}
}

func (rts *RemoteTransactionSubmitter) validateCreatesCallback(clientAction *cmsgs.ClientAction, action *msgs.Action, hashCodes []common.RMId, connections map[common.RMId]*sconn.ServerConnection) error {
	if clientAction.Value().Which() == cmsgs.CLIENTACTIONVALUE_CREATE {
		if !bytes.Equal(clientAction.VarId()[8:], rts.namespace) {
			return fmt.Errorf("Illegal VarId for create: %v", common.MakeTxnId(clientAction.VarId()))
		}
	}
	return nil
}

func (rts *RemoteTransactionSubmitter) SubscriptionConsumer(txn *txnreader.TxnReader, tr *TransactionRecord) error {
	if rts.pendingUpdates != nil {
		rts.pendingUpdates = append(rts.pendingUpdates, func() error {
			return rts.SubscriptionConsumer(txn, tr)
		})
		return nil
	}
	return nil
}

func (rts *RemoteTransactionSubmitter) processPendingUpdates() error {
	updates := rts.pendingUpdates
	rts.pendingUpdates = nil
	for _, update := range updates {
		if err := update(); err != nil {
			return err
		}
	}
	return nil
}

type valueCached struct {
	val []byte
	c   *Cached
}

func (rts *RemoteTransactionSubmitter) filterUpdates(updates *msgs.Update_List, tr *TransactionRecord) map[common.VarUUId]*valueCached {
	results := make(map[common.VarUUId]*valueCached)
	for idx, l := 0, updates.Len(); idx < l; idx++ {
		update := updates.At(idx)
		txnId := common.MakeTxnId(update.TxnId())
		clock := vectorclock.VectorClockFromData(update.Clock(), true)
		actions := txnreader.TxnActionsFromData(update.Actions(), true).Actions()
		utils.DebugLog(rts.logger, "debug", "filterUpdates", "TxnId", txnId, "actionsLen", actions.Len())
		for idy, m := 0, actions.Len(); idy < m; idy++ {
			action := actions.At(idy)
			vUUId := common.MakeVarUUId(action.VarId())
			clockElem := clock.At(vUUId)
			c, found := tr.objs[*vUUId]
			// If the elem is mentioned in objs and we can read the obj
			// then this must go down to the client even if version is
			// zero.
			if !found {
				c, found = rts.cache.m[*vUUId]
				if !found || c.version.IsZero() {
					// If the version is 0 then the client doesn't have the
					// object checked out and isn't interested.
					utils.DebugLog(rts.logger, "TxnId", txnId, "VarUUId", vUUId, "found", found)
					continue
				}
			}
			// if we can't read the object then we must not send any update for it.
			if !c.caps.CanRead() {
				utils.DebugLog(rts.logger, "TxnId", txnId, "VarUUId", vUUId, "canRead", false)
				continue
			}
			value := action.Value()
			switch value.Which() {
			case msgs.ACTIONVALUE_MISSING:
				// In this context, MISSING means we know there was a
				// write of vUUId by txnId, but we have no idea what the
				// value written was. The only safe thing we can do is
				// remove it from the client.
				cmp := c.version.Compare(txnId)
				if cmp == common.EQ && clockElem != c.clockElem {
					panic(fmt.Sprintf("Clock version changed on missing for %v@%v (new:%v != old:%v)", vUUId, txnId, clockElem, c.clockElem))
				}
				utils.DebugLog(rts.logger, "TxnId", txnId, "VarUUId", vUUId, "action", "MISSING", "clockElem", clockElem, "clockElemCached", c.clockElem)
				if clockElem > c.clockElem || (clockElem == c.clockElem && cmp == common.LT) {
					c.version = common.VersionZero
					c.clockElem = 0
					c.refs = nil
					results[*vUUId] = &valueCached{c: c}
				}

			case msgs.ACTIONVALUE_EXISTING:
				cmp := c.version.Compare(txnId)
				if cmp == common.EQ && clockElem != c.clockElem {
					panic(fmt.Sprintf("Clock version changed on missing for %v@%v (new:%v != old:%v)", vUUId, txnId, clockElem, c.clockElem))
				}
				utils.DebugLog(rts.logger, "TxnId", txnId, "VarUUId", vUUId, "action", "WRITEONLY", "clockElem", clockElem, "clockElemCached", c.clockElem)
				if clockElem > c.clockElem || (clockElem == c.clockElem && cmp == common.LT) {
					// If the above condition fails, then the update
					// must pre-date our current knowledge of vUUId. So
					// we're not going to send it to the client in which
					// case the capabilities vUUId grants via its own
					// refs can't widen: we already know everything the
					// client knows and we're not extending that. So
					// it's safe to totally ignore it.
					c.version = txnId
					c.clockElem = clockElem
					modify := value.Existing().Modify()
					if modify.Which() != msgs.ACTIONVALUEEXISTINGMODIFY_WRITE {
						panic(fmt.Sprintf("%v Expected Modify WRITE, but got %v", vUUId, modify.Which()))
					}
					write := modify.Write()
					// depending on internal capnp things, Value() here can
					// return nil instead of an empty slice in some
					// cases. Grrrr.
					value := write.Value()
					if value == nil {
						value = []byte{}
					}
					results[*vUUId] = &valueCached{c: c, val: value}
					c.refs = write.References().ToArray()
					for _, ref := range c.refs {
						vUUId := common.MakeVarUUId(ref.Id())
						caps := common.NewCapability(ref.Capability())
						if c, found := rts.cache.m[*vUUId]; found {
							c.caps = c.caps.Union(caps)
						} else {
							pos := common.Positions(ref.Positions())
							rts.cache.AddCached(vUUId, &Cached{
								VerClock:  VerClock{version: common.VersionZero},
								caps:      caps,
								positions: &pos,
							})
						}
					}
				}

			default:
				panic(fmt.Sprintf("%v Unexpected action value in update from %v: %v", vUUId, txnId, value.Which()))
			}
		}
	}
	return results
}
