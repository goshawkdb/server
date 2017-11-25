package main

import (
	"errors"
	"fmt"
	ui "github.com/jroimartin/gocui"
	"goshawkdb.io/server/debug"
	"os"
)

func maybeExitError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) < 2 {
		maybeExitError(errors.New("Usage: debug path/to/file.log"))
	}
	path := os.Args[1]
	gui, err := debug.NewDebugGui(path)
	maybeExitError(err)
	defer gui.Close()

	if err := gui.MainLoop(); err != ui.ErrQuit {
		maybeExitError(err)
	}
}
