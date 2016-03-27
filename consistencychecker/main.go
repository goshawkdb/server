package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	_ "goshawkdb.io/server/txnengine"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"time"
)

type store struct {
	dir      string
	db       *db.Databases
	rmId     common.RMId
	topology *configuration.Topology
}

type stores []*store

func main() {
	log.SetPrefix(common.ProductName + "ConsistencyChecker ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println(os.Args)

	dirs := os.Args[1:]
	if len(dirs) == 0 {
		log.Fatal("No dirs supplied")
	}

	runtime.GOMAXPROCS(1 + (2 * len(dirs)))

	stores := stores(make([]*store, 0, len(dirs)))
	defer stores.Shutdown()
	for _, dir := range dirs {
		log.Printf("...loading from %v\n", dir)
		store := &store{dir: dir}
		var err error
		if err = store.LoadRMId(); err == nil {
			if err = store.StartDisk(); err == nil {
				err = store.LoadTopology()
			}
		}
		if err != nil {
			log.Println(err)
			return
		}
		stores = append(stores, store)
	}

	if err := stores.CheckEqualTopology(); err != nil {
		log.Println(err)
		return
	}

	if err := stores.IterateVars(func(cell *varWrapperCell) error {
		fmt.Println(cell.vUUId, cell.store)
		return nil
	}); err != nil {
		log.Println(err)
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

func (ss stores) IterateVars(f func(*varWrapperCell) error) error {
	is := &iterateState{
		stores:   ss,
		wrappers: make([]*varWrapper, len(ss)),
		f:        f,
	}
	return is.iterate()
}

func (ss stores) Shutdown() {
	for _, s := range ss {
		s.Shutdown()
	}
}

func (s *store) Shutdown() {
	if s.db == nil {
		return
	}
	s.db.Shutdown()
	s.db = nil
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
	log.Printf("Starting disk server on %v", s.dir)
	disk, err := mdbs.NewMDBServer(s.dir, 0, 0600, server.MDBInitialSize, 1, 10*time.Millisecond, db.DB)
	if err != nil {
		return err
	}
	s.db = disk.(*db.Databases)
	return nil
}

func (s *store) LoadTopology() error {
	res, err := s.db.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		bites, err := rtxn.Get(s.db.Vars, configuration.TopologyVarUUId[:])
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
		bites = s.db.ReadTxnBytesFromDisk(rtxn, txnId)
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
	store   *store
	c       chan *varWrapperCell
	curCell *varWrapperCell
}

type varWrapperCell struct {
	*varWrapper
	vUUId  *common.VarUUId
	varCap *msgs.Var
	err    error
	other  *varWrapperCell
}

func (vw *varWrapper) start() {
	defer close(vw.c)

	c1 := &varWrapperCell{varWrapper: vw}
	c2 := &varWrapperCell{varWrapper: vw}
	c1.other, c2.other = c2, c1

	curCell := c1
	_, err := vw.store.db.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		rtxn.WithCursor(vw.store.db.Vars, func(cursor *mdbs.Cursor) interface{} {
			vUUIdBytes, varBytes, err := cursor.Get(nil, nil, mdb.FIRST)
			if err != nil {
				cursor.Error(fmt.Errorf("Err on finding first var in %v: %v", vw.store, err))
				return nil
			}
			if !bytes.Equal(vUUIdBytes, configuration.TopologyVarUUId[:]) {
				vUUId := common.MakeVarUUId(vUUIdBytes)
				cursor.Error(fmt.Errorf("Err on finding first var in %v: expected to find topology var, but found %v instead! (%v)", vw.store, vUUId, varBytes))
				return nil
			}
			for ; err == nil; vUUIdBytes, varBytes, err = cursor.Get(nil, nil, mdb.NEXT) {
				vUUId := common.MakeVarUUId(vUUIdBytes)
				seg, _, err := capn.ReadFromMemoryZeroCopy(varBytes)
				if err != nil {
					cursor.Error(fmt.Errorf("Err on decoding %v in %v: %v (%v)", vUUId, vw.store, err, varBytes))
					return nil
				}
				varCap := msgs.ReadRootVar(seg)
				curCell.vUUId = vUUId
				curCell.varCap = &varCap
				vw.c <- curCell
				curCell = curCell.other
			}
			if err != nil && err != mdb.NotFound {
				cursor.Error(err)
			}
			return nil
		})
		return nil
	}).ResultError()
	if err != nil {
		curCell.err = err
		vw.c <- curCell
	}
}

func (vw *varWrapper) next() error {
	cell, ok := <-vw.c
	if ok {
		vw.curCell = cell
		return cell.err
	} else {
		vw.curCell = nil
		return nil
	}
}

type iterateState struct {
	stores   stores
	wrappers []*varWrapper
	f        func(*varWrapperCell) error
}

func (is *iterateState) init() {
	for idx, store := range is.stores {
		wrapper := &varWrapper{
			store: store,
			c:     make(chan *varWrapperCell, 0),
		}
		is.wrappers[idx] = wrapper
		go wrapper.start()
	}
}

func (is *iterateState) shutdown() {
	log.Println("Shutting down iterator")
	for _, wrapper := range is.wrappers {
		for wrapper.curCell != nil && wrapper.curCell.err != nil {
			wrapper.next()
		}
	}
}

func (is *iterateState) iterate() error {
	is.init()
	defer is.shutdown()

	for _, wrapper := range is.wrappers {
		if err := wrapper.next(); err != nil {
			return wrapper.curCell.err
		} else if wrapper.curCell.vUUId.Compare(configuration.TopologyVarUUId) == common.EQ {
			if err := wrapper.next(); err != nil {
				return wrapper.curCell.err
			}
		}
	}
	for cell := is.minWrapperCell(); cell != nil; cell = is.minWrapperCell() {
		if err := is.f(cell); err != nil {
			return err
		}
		if err := cell.next(); err != nil {
			return err
		}
	}
	return nil
}

func (is *iterateState) minWrapperCell() *varWrapperCell {
	var cell *varWrapperCell
	for _, wrapper := range is.wrappers {
		switch {
		case wrapper.curCell == nil:
		case cell == nil:
			cell = wrapper.curCell
		case wrapper.curCell.vUUId.Compare(cell.vUUId) == common.LT:
			cell = wrapper.curCell
		}
	}
	return cell
}
