package db

import (
	mdbs "github.com/msackman/gomdb/server"
)

type Databases struct {
	Vars            *mdbs.DBISettings
	Proposers       *mdbs.DBISettings
	BallotOutcomes  *mdbs.DBISettings
	Transactions    *mdbs.DBISettings
	TransactionRefs *mdbs.DBISettings
}

var (
	DB = &Databases{}
)
