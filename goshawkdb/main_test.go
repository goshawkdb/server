package main

import (
	"os"
	"testing"
)

func TestMain(t *testing.T) {
	TrimTestArgs()
	main()
}

func TrimTestArgs() {
	i, l := 0, len(os.Args)
	for ; i < l; i++ {
		if os.Args[i] == "--" {
			break
		}
	}
	if i == l {
		panic("Specify args after --")
	}
	os.Args = append(os.Args[:1], os.Args[i+1:l]...)
}
