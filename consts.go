package server // import "goshawkdb.io/server"

import (
	"time"
)

const (
	ServerVersion      = "dev"
	MDBInitialSize     = 1048576
	TwoToTheSixtyThree = 9223372036854775808

	// for binary backoff in client/remote.go
	SubmissionMinSubmitDelay = 1 * time.Millisecond
	SubmissionMaxSubmitDelay = 2 * time.Second

	// for binary backoff in txnengine/frame.go
	VarRollDelayMin = 50 * time.Millisecond
	VarRollDelayMax = 500 * time.Millisecond

	// input to poisson.P
	VarRollTimeExpectation = 3 * time.Millisecond

	// minimum required P to start a roll
	VarRollPRequirement = 0.9

	// used by rollTranslationCallback to force the roll after this amount of time
	VarRollForceNotFirstAfter = time.Second

	// used by network/connection.go to calculate delays between restarting connections
	ConnectionRestartDelayRangeMS = 5000
	ConnectionRestartDelayMin     = 3 * time.Second

	MostRandomByteIndex     = 7 // will be the lsb of a big-endian client-n in the txnid.
	MigrationBatchElemCount = 64
	PoissonSamples          = 64
	ConfigRootName          = "system:config"
	MetricsRootName         = "system:prometheus"
	HttpProfilePort         = 6060
)
