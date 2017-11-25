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
	v, err := g.SetView(INFO, 0, 2, screenWidth-1, screenHeight-EVENTS_HEIGHT)
	if err != nil {
		if err != ui.ErrUnknownView {
			return err
		}
		v.Wrap = true
		v.Frame = true
		v.Title = "Info"
		if _, err := g.SetCurrentView(HEADERS); err != nil {
			return err
		}
		_, err = g.SetViewOnBottom(INFO)
	}
	row := ip.RowsGui.Selected[ip.RowsGui.highlight]
	maxColName := 0
	for _, col := range ip.Columns {
		if _, found := row[col.Name]; found && len(col.Name) > maxColName {
			maxColName = len(col.Name)
		}
	}
	v.Clear()
	neededHeight := 0
	for _, col := range ip.Columns {
		if val, found := row[col.Name]; found {
			line := ""
			if col.Selected {
				line = fmt.Sprintf("\033[1m%*.*s\033[0m : %s", maxColName, maxColName, col.Name, val)
			} else {
				line = fmt.Sprintf("%*.*s : %s", maxColName, maxColName, col.Name, val)
			}
			fmt.Fprintf(v, "%s\n", line)
			neededHeight++
			// cope with wrapped lines
			neededHeight += len(line) / (screenWidth - 2)
		}
	}
	_, err = g.SetView(INFO, 0, screenHeight-EVENTS_HEIGHT-neededHeight-1, screenWidth-1, screenHeight-EVENTS_HEIGHT)
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
