// +build debug

package utils

import (
	"fmt"
	"github.com/go-kit/kit/log"
)

func init() {
	DebugLog = DebugLogFunc(func(logger log.Logger, keyvals ...interface{}) {
		// it's debug - just force all vals to a string
		for idx := 1; idx < len(keyvals); idx += 2 {
			keyvals[idx] = fmt.Sprint(keyvals[idx])
		}
		if err := logger.Log(keyvals...); err != nil {
			panic(fmt.Sprintf("Error when logging! %v", err))
		}
	})
}
