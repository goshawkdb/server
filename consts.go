package server

import (
	"time"
)

const (
	ServerVersion                 = "dev"
	MDBInitialSize                = 1048576
	TwoToTheSixtyThree            = 9223372036854775808
	SubmissionInitialAttempts     = 0
	SubmissionInitialBackoff      = 2 * time.Microsecond
	SubmissionMaxSubmitDelay      = 2 * time.Second
	VarIdleTimeoutMin             = 500 * time.Millisecond
	VarIdleTimeoutRange           = 250
	FrameLockMinExcessSize        = 100
	FrameLockMinRatio             = 2
	ConnectionRestartDelayRangeMS = 5000
	ConnectionRestartDelayMin     = 3 * time.Second
	MostRandomByteIndex           = 7 // will be the lsb of a big-endian client-n in the txnid.
	MigrationBatchElemCount       = 64
)
