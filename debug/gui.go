package debug

import (
	"fmt"
	ui "github.com/jroimartin/gocui"
)

const (
	HEADERS   = "headers"
	EVENTS    = "events"
	ROWS      = "rows"
	CSELECTOR = "cselector"
	VSELECTOR = "vselector"
	INFO      = "info"

	EVENTS_HEIGHT  = 6
	HEADERS_HEIGHT = 3
)

type DebugGui struct {
	*ui.Gui
	Path           string
	RowsGui        *RowsGui
	Columns        Columns
	ColumnSelector *ColumnSelector
	ValueSelector  *ValueSelector
	InfoPanel      *InfoPanel
	Events         *Events
}

func NewDebugGui(path string) (*DebugGui, error) {
	rows, err := RowsFromFile(path)
	if err != nil {
		return nil, err
	}

	g, err := ui.NewGui(ui.OutputNormal)
	if err != nil {
		return nil, err
	}

	columns := rows.AllColumns()

	dg := &DebugGui{
		Gui:     g,
		Path:    path,
		Columns: columns,
		Events:  &Events{},
	}
	dg.ColumnSelector = &ColumnSelector{DebugGui: dg}
	dg.ValueSelector = &ValueSelector{DebugGui: dg}
	dg.RowsGui = &RowsGui{DebugGui: dg, Rows: rows}
	dg.InfoPanel = &InfoPanel{DebugGui: dg}

	rows.SelectAll()

	dg.SetManager(dg.Events, dg.InfoPanel, dg.Columns, dg.RowsGui, dg.ColumnSelector, dg.ValueSelector)

	if err := dg.setKeybindings(); err != nil {
		return nil, err
	}

	dg.Events.Layout(g) // just to create it so the append will work.
	AppendEvent(g, fmt.Sprintf("Loaded %d rows from %s.", len(rows.All), path))

	return dg, nil
}

func (dg *DebugGui) setKeybindings() error {
	if err := dg.SetKeybinding("", 'q', ui.ModNone, quit); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, ui.KeyArrowDown, ui.ModNone, dg.RowsGui.Down); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, ui.KeyArrowUp, ui.ModNone, dg.RowsGui.Up); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, ui.KeyPgdn, ui.ModNone, dg.RowsGui.PageDown); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, ui.KeyPgup, ui.ModNone, dg.RowsGui.PageUp); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, ui.KeyEnd, ui.ModNone, dg.RowsGui.Bottom); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, ui.KeyHome, ui.ModNone, dg.RowsGui.Top); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, 'a', ui.ModNone, dg.RowsGui.All); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, 'l', ui.ModNone, dg.RowsGui.Limit); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, 's', ui.ModNone, dg.RowsGui.SearchNext); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, 'r', ui.ModNone, dg.RowsGui.SearchPrev); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, ui.KeyEnter, ui.ModNone, dg.RowsGui.StopSearch); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, 't', ui.ModNone, dg.RowsGui.TagToggle); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, 'T', ui.ModNone, dg.RowsGui.TagNone); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, 'L', ui.ModNone, dg.RowsGui.TagLimit); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, ui.KeyArrowRight, ui.ModNone, dg.Columns.Right); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, ui.KeyArrowLeft, ui.ModNone, dg.Columns.Left); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, 'i', ui.ModNone, dg.InfoPanel.Toggle); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, 'c', ui.ModNone, dg.ColumnSelector.Display); err != nil {
		return err
	} else if err := dg.SetKeybinding(HEADERS, 'v', ui.ModNone, dg.ValueSelector.Display); err != nil {
		return err
	} else if err := dg.SetKeybinding(VSELECTOR, 'v', ui.ModNone, dg.ValueSelector.Hide); err != nil {
		return err
	} else if err := dg.SetKeybinding(VSELECTOR, ui.KeyEnter, ui.ModNone, dg.ValueSelector.Limit); err != nil {
		return err
	} else if err := dg.SetKeybinding(VSELECTOR, ui.KeyArrowDown, ui.ModNone, dg.ValueSelector.Down); err != nil {
		return err
	} else if err := dg.SetKeybinding(VSELECTOR, ui.KeyArrowUp, ui.ModNone, dg.ValueSelector.Up); err != nil {
		return err
	} else if err := dg.SetKeybinding(VSELECTOR, ui.KeyPgdn, ui.ModNone, dg.ValueSelector.PageDown); err != nil {
		return err
	} else if err := dg.SetKeybinding(VSELECTOR, ui.KeyPgup, ui.ModNone, dg.ValueSelector.PageUp); err != nil {
		return err
	} else if err := dg.SetKeybinding(VSELECTOR, ui.KeyEnd, ui.ModNone, dg.ValueSelector.Bottom); err != nil {
		return err
	} else if err := dg.SetKeybinding(VSELECTOR, ui.KeyHome, ui.ModNone, dg.ValueSelector.Top); err != nil {
		return err
	} else if err := dg.SetKeybinding(CSELECTOR, 'c', ui.ModNone, dg.ColumnSelector.Hide); err != nil {
		return err
	} else if err := dg.SetKeybinding(CSELECTOR, 'h', ui.ModNone, dg.ColumnSelector.HideEmpty); err != nil {
		return err
	} else if err := dg.SetKeybinding(CSELECTOR, 'a', ui.ModNone, dg.ColumnSelector.ShowAll); err != nil {
		return err
	} else if err := dg.SetKeybinding(CSELECTOR, ui.KeyArrowDown, ui.ModNone, dg.ColumnSelector.CursorDown); err != nil {
		return err
	} else if err := dg.SetKeybinding(CSELECTOR, ui.KeyArrowUp, ui.ModNone, dg.ColumnSelector.CursorUp); err != nil {
		return err
	} else if err := dg.SetKeybinding(CSELECTOR, ui.KeyPgdn, ui.ModNone, dg.ColumnSelector.MoveDown); err != nil {
		return err
	} else if err := dg.SetKeybinding(CSELECTOR, ui.KeyPgup, ui.ModNone, dg.ColumnSelector.MoveUp); err != nil {
		return err
	}

	return nil
}

func quit(g *ui.Gui, v *ui.View) error {
	return ui.ErrQuit
}
