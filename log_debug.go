// +build debug

package server

import "log"

func init() {
	Log = LogFunc(log.Println)
}
