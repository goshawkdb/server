package main

import (
	"flag"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/db"
	eng "goshawkdb.io/server/txnengine"
	"log"
	"os"
	"runtime"
	"time"
)

func main() {
	log.SetPrefix(common.ProductName + " ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println(os.Args)

	procs := runtime.NumCPU()
	if procs < 2 {
		procs = 2
	}
	runtime.GOMAXPROCS(procs)

	var vUUIdStr string
	flag.StringVar(&vUUIdStr, "var", "", "var to interrogate")
	flag.Parse()

	dirs := flag.Args()
	if len(dirs) == 0 {
		log.Fatal("No dirs supplied")
	}

	vars := make(map[common.VarUUId]*varstate)
	for _, d := range dirs {
		dir := d
		log.Printf("...loading from %v\n", dir)
		disk, err := mdbs.NewMDBServer(dir, 0, 0600, server.OneTB, 1, time.Millisecond, db.DB)
		if err != nil {
			log.Println(err)
			continue
		}
		loadVars(disk, vars)
		disk.Shutdown()
	}

	log.Printf("Found %v unique vars", len(vars))

	if vUUIdStr != "" {
		vUUId := common.MakeVarUUIdFromStr(vUUIdStr)
		if vUUId == nil {
			log.Printf("Unable to parse %v as vUUId\n", vUUIdStr)
		}
		if state, found := vars[*vUUId]; found {
			log.Println(state)
		} else {
			log.Printf("Unable to find %v\n", vUUId)
		}
	}
}

func loadVars(disk *mdbs.MDBServer, vars map[common.VarUUId]*varstate) {
	_, err := disk.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		_, err := rtxn.WithCursor(db.DB.Vars, func(cursor *mdbs.Cursor) interface{} {
			key, data, err := cursor.Get(nil, nil, mdb.FIRST)
			for ; err == nil; key, data, err = cursor.Get(nil, nil, mdb.NEXT) {
				vUUId := common.MakeVarUUId(key)
				seg, _, err := capn.ReadFromMemoryZeroCopy(data)
				if err != nil {
					log.Printf("Error when decoding %v: %v\n", vUUId, err)
					continue
				}

				varCap := msgs.ReadRootVar(seg)
				pos := varCap.Positions()
				positions := (*common.Positions)(&pos)
				writeTxnId := common.MakeTxnId(varCap.WriteTxnId())
				writeTxnClock := eng.VectorClockFromCap(varCap.WriteTxnClock())
				writesClock := eng.VectorClockFromCap(varCap.WritesClock())

				if state, found := vars[*vUUId]; found {
					if err := state.matches(disk, writeTxnId, writeTxnClock, writesClock, positions); err != nil {
						log.Println(err)
					}
				} else {
					state = &varstate{
						vUUId:            vUUId,
						disks:            []*mdbs.MDBServer{disk},
						writeTxnId:       writeTxnId,
						writeTxnClock:    writeTxnClock,
						writeWritesClock: writesClock,
						positions:        positions,
					}
					vars[*vUUId] = state
				}
			}
			return nil
		})
		return err
	}).ResultError()
	if err != nil {
		log.Println(err)
	}
}

type varstate struct {
	vUUId            *common.VarUUId
	disks            []*mdbs.MDBServer
	writeTxnId       *common.TxnId
	writeTxnClock    *eng.VectorClock
	writeWritesClock *eng.VectorClock
	positions        *common.Positions
}

func (vs *varstate) matches(disk *mdbs.MDBServer, writeTxnId *common.TxnId, writeTxnClock, writesClock *eng.VectorClock, positions *common.Positions) error {
	if !vs.writeTxnId.Equal(writeTxnId) {
		return fmt.Errorf("%v TxnId divergence: %v vs %v", vs.vUUId, vs.writeTxnId, writeTxnId)
	}
	if !vs.positions.Equal(positions) {
		return fmt.Errorf("%v positions divergence: %v vs %v", vs.vUUId, vs.positions, positions)
	}
	if !vs.writeTxnClock.Equal(writeTxnClock) {
		return fmt.Errorf("%v Txn %v Clock divergence: %v vs %v", vs.vUUId, vs.writeTxnId, vs.writeTxnClock, writeTxnClock)
	}
	if !vs.writeWritesClock.Equal(writesClock) {
		return fmt.Errorf("%v Txn %v WritesClock divergence: %v vs %v", vs.vUUId, vs.writeTxnId, vs.writeWritesClock, writesClock)
	}
	vs.disks = append(vs.disks, disk)
	return nil
}

func (vs *varstate) String() string {
	return fmt.Sprintf("%v found in %v stores:\n positions:\t%v\n writeTxnId:\t%v\n writeTxnClock:\t%v\n writesClock:\t%v\n", vs.vUUId, len(vs.disks), vs.positions, vs.writeTxnId, vs.writeTxnClock, vs.writeWritesClock)
}
