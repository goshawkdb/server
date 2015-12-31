package db

import (
	"encoding/binary"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	// "fmt"
	capn "github.com/glycerine/go-capnproto"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
)

func init() {
	DB.Transactions = &mdbs.DBISettings{Flags: mdb.CREATE}
	DB.TransactionRefs = &mdbs.DBISettings{Flags: mdb.CREATE}
}

func TxnToRootBytes(txn *msgs.Txn) []byte {
	seg := capn.NewBuffer(nil)
	txnCap := msgs.NewRootTxn(seg)
	txnCap.SetId(txn.Id())
	txnCap.SetRetry(txn.Retry())
	txnCap.SetSubmitter(txn.Submitter())
	txnCap.SetSubmitterBootCount(txn.SubmitterBootCount())
	txnCap.SetActions(txn.Actions())
	txnCap.SetAllocations(txn.Allocations())
	txnCap.SetFInc(txn.FInc())
	txnCap.SetTopologyVersion(txn.TopologyVersion())

	return server.SegToBytes(seg)
}

func WriteTxnToDisk(rwtxn *mdbs.RWTxn, txnId *common.TxnId, txnBites []byte) error {
	bites, err := rwtxn.Get(DB.TransactionRefs, txnId[:])

	switch err {
	case nil:
		count := binary.BigEndian.Uint32(bites) + 1
		// fmt.Printf("%v +Refcount now %v\n", txnId, count)
		binary.BigEndian.PutUint32(bites, count)
		return rwtxn.Put(DB.TransactionRefs, txnId[:], bites, 0)

	case mdb.NotFound:
		if err = rwtxn.Put(DB.Transactions, txnId[:], txnBites, 0); err != nil {
			return err
		}

		bites = []byte{0, 0, 0, 0}
		binary.BigEndian.PutUint32(bites, 1)
		// fmt.Printf("%v +Refcount now 1\n", txnId)
		return rwtxn.Put(DB.TransactionRefs, txnId[:], bites, 0)

	default:
		return err
	}
}

func ReadTxnBytesFromDisk(rtxn *mdbs.RTxn, txnId *common.TxnId) []byte {
	bites, err := rtxn.Get(DB.Transactions, txnId[:])
	if err == nil {
		return bites
	} else {
		return nil
	}
}

func DeleteTxnFromDisk(rwtxn *mdbs.RWTxn, txnId *common.TxnId) error {
	bites, err := rwtxn.Get(DB.TransactionRefs, txnId[:])

	switch err {
	case nil:
		if count := binary.BigEndian.Uint32(bites) - 1; count == 0 {
			// fmt.Printf("%v -Refcount now 0\n", txnId)
			if err = rwtxn.Del(DB.TransactionRefs, txnId[:], nil); err != nil {
				return err
			}
			return rwtxn.Del(DB.Transactions, txnId[:], nil)

		} else {
			// fmt.Printf("%v -Refcount now %v\n", txnId, count)
			binary.BigEndian.PutUint32(bites, count)
			return rwtxn.Put(DB.TransactionRefs, txnId[:], bites, 0)
		}
	case mdb.NotFound:
		return nil
	default:
		return err
	}
}
