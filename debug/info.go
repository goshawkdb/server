package debug

import (
	"fmt"
	ui "github.com/jroimartin/gocui"
)

type InfoPanel struct {
	*DebugGui
	Displayed bool
}

func (ip *InfoPanel) Layout(g *ui.Gui) error {
	if !ip.Displayed {
		return nil
	}
	screenWidth, screenHeight := g.Size()
	top := HEADERS_HEIGHT - 1
	bottom := screenHeight - EVENTS_HEIGHT
	v, err := g.SetView(INFO, 0, top, screenWidth-1, bottom)
	if err != nil {
		if err != ui.ErrUnknownView {
			return err
		}
		v.Wrap = true
		v.Frame = true
		v.Title = "Info (i to hide)"
		if _, err := g.SetCurrentView(HEADERS); err != nil {
			return err
		}
		if _, err = g.SetViewOnTop(EVENTS); err != nil {
			return err
		}
	}
	row := ip.RowsGui.Selected[ip.RowsGui.highlight]
	maxColName := 0
	for _, col := range ip.Columns {
		if _, found := row[col.Name]; found && len(col.Name) > maxColName {
			maxColName = len(col.Name)
		}
	}
	v.Clear()
	for _, col := range ip.Columns {
		if val, found := row[col.Name]; found {
			if col.Selected {
				fmt.Fprintf(v, "\033[1m%*.*s\033[0m : %s\n", maxColName, maxColName, col.Name, val)
			} else {
				fmt.Fprintf(v, "%*.*s : %s\n", maxColName, maxColName, col.Name, val)
			}
		}
	}
	// we determine neededHeight this way because it will cope with multiline rows better
	neededHeight := 0
	for idx := 0; true; idx++ {
		line, err := v.Line(idx)
		if err != nil {
			break
		}
		neededHeight++
		// cope with wrapped lines
		neededHeight += (len(line) - 1) / (screenWidth - 2)
	}
	maxHeight := bottom - top
	if neededHeight > maxHeight {
		neededHeight = maxHeight
	}
	top = bottom - neededHeight

	_, err = g.SetView(INFO, 0, top, screenWidth-1, bottom)
	if err != nil {
		return err
	}
	// now that we've figured out our own height, we need to get the RowsGui to relayout itself.
	return ip.RowsGui.Layout(g)
}

func (ip *InfoPanel) Toggle(g *ui.Gui, v *ui.View) error {
	ip.Displayed = !ip.Displayed
	if ip.Displayed {
		return ip.RowsGui.SetHighlight(ip.RowsGui.highlight, g)
	} else {
		return g.DeleteView(INFO)
	}
}
