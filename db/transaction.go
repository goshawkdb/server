package db

import (
	"encoding/binary"
	"goshawkdb.io/common"
	// "fmt"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
)

func init() {
	DB.Transactions = &mdbs.DBISettings{Flags: mdb.CREATE}
	DB.TransactionRefs = &mdbs.DBISettings{Flags: mdb.CREATE}
}

func (db *Databases) WriteTxnToDisk(rwtxn *mdbs.RWTxn, txnId *common.TxnId, txnBites []byte) error {
	bites, err := rwtxn.Get(db.TransactionRefs, txnId[:])

	switch err {
	case nil:
		count := binary.BigEndian.Uint32(bites) + 1
		// fmt.Printf("%v +Refcount now %v\n", txnId, count)
		binary.BigEndian.PutUint32(bites, count)
		return rwtxn.Put(db.TransactionRefs, txnId[:], bites, 0)

	case mdb.NotFound:
		if err = rwtxn.Put(db.Transactions, txnId[:], txnBites, 0); err != nil {
			return err
		}

		bites = []byte{0, 0, 0, 0}
		binary.BigEndian.PutUint32(bites, 1)
		// fmt.Printf("%v +Refcount now 1\n", txnId)
		return rwtxn.Put(db.TransactionRefs, txnId[:], bites, 0)

	default:
		return err
	}
}

func (db *Databases) IncrTxnRefCount(rwtxn *mdbs.RWTxn, txnId *common.TxnId) error {
	bites, err := rwtxn.Get(db.TransactionRefs, txnId[:])

	if err == nil {
		count := binary.BigEndian.Uint32(bites) + 1
		// fmt.Printf("%v +Refcount now %v\n", txnId, count)
		binary.BigEndian.PutUint32(bites, count)
		return rwtxn.Put(db.TransactionRefs, txnId[:], bites, 0)

	} else {
		return err // includes not found
	}
}

func (db *Databases) ReadTxnBytesFromDisk(rtxn *mdbs.RTxn, txnId *common.TxnId) []byte {
	bites, err := rtxn.Get(db.Transactions, txnId[:])
	if err == nil {
		return bites
	} else {
		return nil
	}
}

func (db *Databases) DeleteTxnFromDisk(rwtxn *mdbs.RWTxn, txnId *common.TxnId) error {
	bites, err := rwtxn.Get(db.TransactionRefs, txnId[:])

	switch err {
	case nil:
		if count := binary.BigEndian.Uint32(bites) - 1; count == 0 {
			// fmt.Printf("%v -Refcount now 0\n", txnId)
			if err = rwtxn.Del(db.TransactionRefs, txnId[:], nil); err != nil {
				return err
			}
			return rwtxn.Del(db.Transactions, txnId[:], nil)

		} else {
			// fmt.Printf("%v -Refcount now %v\n", txnId, count)
			binary.BigEndian.PutUint32(bites, count)
			return rwtxn.Put(db.TransactionRefs, txnId[:], bites, 0)
		}
	case mdb.NotFound:
		return nil
	default:
		return err
	}
}
