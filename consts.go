package server

import (
	"time"
)

const (
	ServerVersion                 = "dev"
	MDBInitialSize                = 1048576
	TwoToTheSixtyThree            = 9223372036854775808
	SubmissionInitialBackoff      = 2 * time.Microsecond
	SubmissionMaxSubmitDelay      = 2 * time.Second
	VarRollDelayMin               = 50 * time.Millisecond
	VarRollDelayMax               = 500 * time.Millisecond
	VarRollTimeExpectation        = 2 * time.Millisecond
	VarRollPRequirement           = 0.9
	ConnectionRestartDelayRangeMS = 5000
	ConnectionRestartDelayMin     = 3 * time.Second
	MostRandomByteIndex           = 7 // will be the lsb of a big-endian client-n in the txnid.
	MigrationBatchElemCount       = 64
	PoissonSamples                = 64
)
