package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	eng "goshawkdb.io/server/txnengine"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"time"
)

type store struct {
	dir      string
	disk     *mdbs.MDBServer
	rmId     common.RMId
	topology *configuration.Topology
}

type stores []*store

func main() {
	log.SetPrefix(common.ProductName + "ConsistencyChecker ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println(os.Args)

	procs := runtime.NumCPU()
	if procs < 2 {
		procs = 2
	}
	runtime.GOMAXPROCS(procs)

	dirs := flag.Args()
	if len(dirs) == 0 {
		log.Fatal("No dirs supplied")
	}

	stores := stores(make([]*store, 0, len(dirs)))
	defer stores.Shutdown()
	for _, d := range dirs {
		log.Printf("...loading from %v\n", d)
		store := &store{dir: d}
		var err error
		if err = store.LoadRMId(); err == nil {
			if err = store.StartDisk(); err == nil {
				err = store.LoadTopology()
			}
		}
		if err != nil {
			log.Println(err)
		}
	}

	if err := stores.CheckEqualTopology(); err != nil {
		log.Println(err)
		return
	}

}

func (ss stores) CheckEqualTopology() error {
	var first *store
	for idx, s := range ss {
		if idx == 0 {
			first = s
		} else if !first.topology.Configuration.Equal(s.topology.Configuration) {
			return fmt.Errorf("Unequal topologies: %v has %v; %v has %v",
				first, first.topology, s, s.topology)
		}
	}
	return nil
}

func (ss stores) IterateVars(f func(*varWrapper) error) error {
	is := &iterateState{
		stores:   ss,
		wrappers: make([]*varWrapper, 0, len(ss)),
		f:        f,
	}
	return is.init()
}

func (ss stores) Shutdown() {
	for _, s := range ss {
		s.Shutdown()
	}
}

func (s *store) Shutdown() {
	if s.disk == nil {
		return
	}
	s.disk.Shutdown()
	s.disk = nil
}

func (s *store) String() string {
	return fmt.Sprintf("%v(%v)", s.rmId, s.dir)
}

func (s *store) LoadRMId() error {
	rmIdBytes, err := ioutil.ReadFile(s.dir + "/rmid")
	if err != nil {
		return err
	}
	s.rmId = common.RMId(binary.BigEndian.Uint32(rmIdBytes))
	return nil
}

func (s *store) StartDisk() error {
	disk, err := mdbs.NewMDBServer(s.dir, 0, 0600, server.MDBInitialSize, 1, time.Millisecond, db.DB)
	if err != nil {
		return err
	}
	s.disk = disk
	return nil
}

func (s *store) LoadTopology() error {
	res, err := s.disk.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		bites, err := rtxn.Get(db.DB.Vars, configuration.TopologyVarUUId[:])
		if err != nil {
			rtxn.Error(err)
			return nil
		}
		seg, _, err := capn.ReadFromMemoryZeroCopy(bites)
		if err != nil {
			rtxn.Error(err)
			return nil
		}
		varCap := msgs.ReadRootVar(seg)
		txnId := common.MakeTxnId(varCap.WriteTxnId())
		bites = db.ReadTxnBytesFromDisk(rtxn, txnId)
		if bites == nil {
			rtxn.Error(fmt.Errorf("Unable to find txn for topology: %v", txnId))
			return nil
		}
		seg, _, err = capn.ReadFromMemoryZeroCopy(bites)
		if err != nil {
			rtxn.Error(err)
			return nil
		}
		txnCap := msgs.ReadRootTxn(seg)
		actions := txnCap.Actions()
		if actions.Len() != 1 {
			rtxn.Error(fmt.Errorf("Topology txn has %v actions; expected 1", actions.Len()))
			return nil
		}
		action := actions.At(0)
		var refs msgs.VarIdPos_List
		switch action.Which() {
		case msgs.ACTION_WRITE:
			w := action.Write()
			bites = w.Value()
			refs = w.References()
		case msgs.ACTION_READWRITE:
			rw := action.Readwrite()
			bites = rw.Value()
			refs = rw.References()
		case msgs.ACTION_CREATE:
			c := action.Create()
			bites = c.Value()
			refs = c.References()
		default:
			rtxn.Error(fmt.Errorf("Expected topology txn action to be w, rw, or c; found %v", action.Which()))
			return nil
		}

		if refs.Len() != 1 {
			rtxn.Error(fmt.Errorf("Topology txn action has %v references; expected 1", refs.Len()))
			return nil
		}
		rootRef := refs.At(0)

		seg, _, err = capn.ReadFromMemoryZeroCopy(bites)
		if err != nil {
			rtxn.Error(err)
			return nil
		}
		topology, err := configuration.TopologyFromCap(txnId, &rootRef, bites)
		if err != nil {
			rtxn.Error(err)
			return nil
		}
		return topology
	}).ResultError()
	if err != nil {
		return err
	}
	s.topology = res.(*configuration.Topology)
	return nil
}

type varWrapper struct {
	*iterateState
	vUUId  *common.VarUUId
	rtxn   *mdbs.RTxn
	cursor *mdbs.Cursor
}

type iterateState struct {
	stores   stores
	wrappers []*varWrapper
	f        func(*varWrapper) error
}

func (is *iterateState) init() error {
	if len(is.stores) == len(is.wrappers) {
		return nil // actually do nextState
	}
	s := is.stores[len(is.wrappers)]
	_, err := s.disk.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		rtxn.WithCursor(db.DB.Vars, func(cursor *mdbs.Cursor) interface{} {
			is.wrappers = append(is.wrappers, &varWrapper{
				iterateState: is,
				rtxn:         rtxn,
				cursor:       cursor,
			})
			return is.init()
		})
		return nil
	}).ResultError()
	return err

}
