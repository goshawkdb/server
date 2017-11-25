package debug

import (
	"fmt"
	ui "github.com/jroimartin/gocui"
)

type ValueSelector struct {
	*DebugGui
	Displayed bool
	Key       string
	Values    []string
	From      int
	Highlight int
}

func (vs *ValueSelector) Layout(g *ui.Gui) error {
	if !vs.Displayed {
		return nil
	}
	screenWidth, screenHeight := g.Size()

	desiredWidth := 0
	for _, val := range vs.Values {
		if len(val) > desiredWidth {
			desiredWidth = len(val)
		}
	}
	desiredWidth += 3
	if desiredWidth >= screenWidth {
		desiredWidth = screenWidth - 1
	}
	desiredHeight := len(vs.Values) + 1
	if desiredHeight >= screenHeight {
		desiredHeight = screenHeight - 1
	}
	midX, midY := screenWidth/2, screenHeight/2
	midWR, midWL := desiredWidth/2, desiredWidth-desiredWidth/2
	midHB, midHT := desiredHeight/2, desiredHeight-desiredHeight/2

	v, err := g.SetView(VSELECTOR, midX-midWL, midY-midHT, midX+midWR, midY+midHB)
	if err != nil {
		if err != ui.ErrUnknownView {
			return err
		}
		v.Frame = true
		v.Title = "Select Value to Limit (v to hide)"
	}
	v.Clear()
	end := vs.From + screenHeight
	if end > len(vs.Values) {
		end = len(vs.Values)
	}
	for idx, val := range vs.Values[vs.From:end] {
		if idx+vs.From == vs.Highlight {
			fmt.Fprintf(v, " \033[7m%s\033[0m\n", val)
		} else {
			fmt.Fprintf(v, " %s\n", val)
		}
	}
	if _, err := g.SetCurrentView(VSELECTOR); err != nil {
		return err
	}
	return nil
}

func (vs *ValueSelector) Display(g *ui.Gui, v *ui.View) error {
	vs.Displayed = true
	vs.Highlight = 0
	vs.From = 0
	key := ""
	for _, col := range vs.Columns {
		if col.Selected {
			key = col.Name
			break
		}
	}
	vs.Key = key
	vs.Values = vs.RowsGui.Values(key)

	return nil
}

func (vs *ValueSelector) Hide(g *ui.Gui, v *ui.View) error {
	vs.Displayed = false
	vs.Key = ""
	vs.Values = nil
	if err := g.DeleteView(VSELECTOR); err != nil {
		return err
	}
	if _, err := g.SetCurrentView(HEADERS); err != nil {
		return err
	}
	return nil
}

func (vs *ValueSelector) SetHighlight(h int, v *ui.View) error {
	if h < 0 {
		h = 0
	} else if h >= len(vs.Values) {
		h = len(vs.Values) - 1
	}
	vs.Highlight = h
	_, height := v.Size()
	to := vs.From + height
	if vs.Highlight >= to {
		vs.From = vs.Highlight - height + 1
	} else if vs.Highlight < vs.From {
		vs.From = vs.Highlight
	}
	return nil
}

func (vs *ValueSelector) Down(g *ui.Gui, v *ui.View) error {
	return vs.SetHighlight(vs.Highlight+1, v)
}

func (vs *ValueSelector) Up(g *ui.Gui, v *ui.View) error {
	return vs.SetHighlight(vs.Highlight-1, v)
}

func (vs *ValueSelector) Top(g *ui.Gui, v *ui.View) error {
	return vs.SetHighlight(0, v)
}

func (vs *ValueSelector) Bottom(g *ui.Gui, v *ui.View) error {
	return vs.SetHighlight(len(vs.Values)-1, v)
}

func (vs *ValueSelector) PageDown(g *ui.Gui, v *ui.View) error {
	_, height := v.Size()
	height--
	height /= 2
	if height == 0 {
		height = 1
	}
	return vs.SetHighlight(vs.Highlight+height, v)
}

func (vs *ValueSelector) PageUp(g *ui.Gui, v *ui.View) error {
	_, height := v.Size()
	height--
	height /= 2
	if height == 0 {
		height = 1
	}
	return vs.SetHighlight(vs.Highlight-height, v)
}

func (vs *ValueSelector) Limit(g *ui.Gui, v *ui.View) error {
	v, err := g.View(VSELECTOR)
	if err != nil {
		return err
	}
	val := vs.Values[vs.Highlight]
	vs.RowsGui.LimitSelected(vs.Key, val)
	if err := AppendEvent(g, fmt.Sprintf("Added constraint %s=%s. %d matching rows", vs.Key, val, len(vs.RowsGui.Selected))); err != nil {
		return err
	}
	if err := vs.RowsGui.SetHighlight(0, g); err != nil {
		return err
	}
	return vs.Hide(g, v)
}
