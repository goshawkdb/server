package utils

import (
	"github.com/go-kit/kit/log"
)

func CheckWarn(e error, logger log.Logger) bool {
	if e != nil {
		logger.Log("msg", "Warning", "error", e)
		return true
	}
	return false
}

type DebugLogFunc func(log.Logger, ...interface{})

var DebugLog = DebugLogFunc(func(log.Logger, ...interface{}) {})

type EmptyStruct struct{}

var EmptyStructVal = EmptyStruct{}

func (es EmptyStruct) String() string { return "" }
