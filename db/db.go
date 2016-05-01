package db

import (
	mdbs "github.com/msackman/gomdb/server"
)

type Databases struct {
	*mdbs.MDBServer
	Vars            *mdbs.DBISettings
	Proposers       *mdbs.DBISettings
	BallotOutcomes  *mdbs.DBISettings
	Transactions    *mdbs.DBISettings
	TransactionRefs *mdbs.DBISettings
}

var (
	DB = &Databases{}
)

func (db *Databases) Clone() mdbs.DBIsInterface {
	return &Databases{
		Vars:            db.Vars.Clone(),
		Proposers:       db.Proposers.Clone(),
		BallotOutcomes:  db.BallotOutcomes.Clone(),
		Transactions:    db.Transactions.Clone(),
		TransactionRefs: db.TransactionRefs.Clone(),
	}
}

func (db *Databases) SetServer(server *mdbs.MDBServer) {
	db.MDBServer = server
}
