package main

import (
	"errors"
	"fmt"
	ui "github.com/jroimartin/gocui"
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
	rows, err := RowsFromFile(path)
	maybeExitError(err)
	g, err := NewDebugGui(rows)
	maybeExitError(err)
	defer g.Close()

	if err := g.MainLoop(); err != nil && err != ui.ErrQuit {
		maybeExitError(err)
	}
}
