package debug

import (
	"fmt"
	ui "github.com/jroimartin/gocui"
	"strconv"
	"strings"
)

type Column struct {
	Name      string
	Displayed bool
	Selected  bool
	Width     int
	XStart    int
}

type Columns []*Column

func (cs Columns) ToggleDisplayed(idx int) {
	c := cs[idx]
	c.Displayed = !c.Displayed
	if c.Selected {
		c.Selected = false
		// attempt to select the next displayed column on the right
		for _, right := range cs[idx+1:] {
			if right.Displayed {
				right.Selected = true
				return
			}
		}
		// failed going right, so now go left
		for idx--; idx >= 0; idx-- {
			left := cs[idx]
			if left.Displayed {
				left.Selected = true
				return
			}
		}
		// failed again, so just have rule you can't turn off everything!
		c.Displayed = true
		c.Selected = true
	}
}

func (cs Columns) Left(g *ui.Gui, v *ui.View) error {
	var left *Column
	for _, c := range cs {
		if !c.Displayed {
			continue
		}
		if left != nil && c.Selected {
			c.Selected = false
			left.Selected = true
			return nil
		}
		left = c
	}
	return nil
}

func (cs Columns) Right(g *ui.Gui, v *ui.View) error {
	var left *Column
	for _, c := range cs {
		if !c.Displayed {
			continue
		}
		if left != nil && left.Selected {
			left.Selected = false
			c.Selected = true
			return nil
		}
		left = c
	}
	return nil
}

func (cs Columns) Layout(g *ui.Gui) error {
	screenWidth, _ := g.Size()
	v, err := g.SetView(HEADERS, 0, 0, screenWidth-1, 2)
	if err != nil {
		if err == ui.ErrUnknownView {
			if _, err := g.SetCurrentView(HEADERS); err != nil {
				return err
			}
		} else {
			return err
		}
		v.Wrap = false
		v.Frame = true
		v.Title = "Columns (c to select)"
	}

	viewWidth := screenWidth - 2
	ox, _ := v.Origin()

	v.Clear()
	x := 0
	for _, c := range cs {
		if !c.Displayed {
			continue
		}
		c.XStart = x
		x += c.Width
		label := c.Name
		labelWidth := c.Width
		if l := len(label); l > labelWidth {
			label = label[:labelWidth]
		} else if l < labelWidth {
			suffix := strings.Repeat(" ", labelWidth-l)
			label += suffix
		}
		if c.Selected {
			fmt.Fprintf(v, "\033[1m%s\033[0m", label)

			// We must ensure the selected column is on the screen.  It's
			// more important to get the lhs of the column on the screen
			// than the rhs.
			if viewWidth <= c.Width || ox >= c.XStart {
				// screen too small  || origin too high to get lhs of column
				ox = c.XStart
			} else if x-ox > viewWidth {
				// rhs of column is on the right of the screen

				// Given that we have enough space to fix the whole column
				// on the screen, we should make sure the rhs is on the
				// screen as well as the left.
				ox = x - viewWidth
			}
		} else {
			fmt.Fprintf(v, "%s", label)
		}
	}

	if viewWidth >= (x-ox) && ox > 0 { // x-ox is from origin to end of columns
		// We have space left over. We should try reducing the
		// origin to fit more columns in.
		ox = x - viewWidth
		if ox < 0 {
			ox = 0
		}
	}
	return v.SetOrigin(ox, 0)
}

type ColumnSelector struct {
	*DebugGui
	Displayed bool
}

func (cs *ColumnSelector) Layout(g *ui.Gui) error {
	if !cs.Displayed {
		return nil
	}
	screenWidth, screenHeight := g.Size()

	columnCount := len(cs.Columns)
	desiredHeight := columnCount + 1
	desiredWidth := 0
	for _, c := range cs.Columns {
		if len(c.Name) > desiredWidth {
			desiredWidth = len(c.Name)
		}
	}
	columnCountWidth := len(fmt.Sprintf("%d", columnCount))
	desiredWidth += len(fmt.Sprintf(" %*d. [x]   ", columnCountWidth, columnCount))
	if desiredWidth >= screenWidth {
		desiredWidth = screenWidth - 1
	}
	if desiredHeight >= screenHeight {
		desiredHeight = screenHeight - 1
	}
	midX, midY := screenWidth/2, screenHeight/2
	midWR, midWL := desiredWidth/2, desiredWidth-desiredWidth/2
	midHB, midHT := desiredHeight/2, desiredHeight-desiredHeight/2

	v, err := g.SetView(CSELECTOR, midX-midWL, midY-midHT, midX+midWR, midY+midHB)
	if err != nil {
		if err != ui.ErrUnknownView {
			return err
		}
		v.Frame = true
		v.Title = "Select Columns (c to hide)"
		v.Editor = cs
		v.Editable = true
		if columnCount > 0 {
			v.SetCursor(columnCountWidth+4, 0)
		}
	}
	v.Clear()
	for idx, c := range cs.Columns {
		d := " "
		if c.Displayed {
			d = "X"
		}
		fmt.Fprintf(v, " %*d. [%s] %s\n", columnCountWidth, idx, d, c.Name)
	}
	if _, err := g.SetCurrentView(CSELECTOR); err != nil {
		return err
	}
	return nil
}

func (cs *ColumnSelector) Display(g *ui.Gui, v *ui.View) error {
	g.Cursor = true
	cs.Displayed = true
	return nil
}

func (cs *ColumnSelector) Hide(g *ui.Gui, v *ui.View) error {
	g.Cursor = false
	cs.Displayed = false
	if err := g.DeleteView(CSELECTOR); err != nil {
		return err
	}
	if _, err := g.SetCurrentView(HEADERS); err != nil {
		return err
	}
	return nil
}

func (cs *ColumnSelector) ShowAll(g *ui.Gui, v *ui.View) error {
	for _, col := range cs.Columns {
		col.Displayed = true
	}
	return AppendEvent(g, "Showing all columns.")
}

// this does not enable non-empty columns - only hides empty ones
func (cs *ColumnSelector) HideEmpty(g *ui.Gui, v *ui.View) error {
	zeros := make([]int, len(cs.Columns))
	keys := make(map[string]*int, len(cs.Columns))
	for idx, col := range cs.Columns {
		keys[col.Name] = &zeros[idx]
	}
	for _, row := range cs.RowsGui.Selected {
		for k := range row {
			cPtr, found := keys[k]
			if found {
				*cPtr++
			}
		}
	}
	var oldSelectedIdx int
	selectedNeeded := false
	for idx, col := range cs.Columns {
		if cPtr := keys[col.Name]; *cPtr == 0 {
			col.Displayed = false
			if col.Selected {
				col.Selected = false
				oldSelectedIdx = idx
			}
		} else if col.Displayed && selectedNeeded {
			col.Selected = true
			selectedNeeded = false
		}
	}
	if selectedNeeded {
		for idx := oldSelectedIdx - 1; idx >= 0; idx-- {
			col := cs.Columns[idx]
			if col.Displayed {
				col.Selected = true
				break
			}
		}
	}
	return AppendEvent(g, "Hidden empty columns.")
}

func (cs *ColumnSelector) CursorDown(g *ui.Gui, v *ui.View) error {
	cx, cy := v.Cursor()
	ox, oy := v.Origin()
	lineNum := cy + oy + 1
	if lineNum >= len(cs.Columns) {
		return nil
	}
	_, h := v.Size()
	if cy == h-1 {
		return v.SetOrigin(ox, oy+1)
	} else {
		v.SetCursor(cx, cy+1)
	}
	return nil
}

func (cs *ColumnSelector) CursorUp(g *ui.Gui, v *ui.View) error {
	cx, cy := v.Cursor()
	ox, oy := v.Origin()
	lineNum := cy + oy - 1
	if lineNum < 0 {
		return nil
	}
	if cy == 0 {
		return v.SetOrigin(ox, oy-1)
	} else {
		v.SetCursor(cx, cy-1)
	}
	return nil
}

func (cs *ColumnSelector) MoveDown(g *ui.Gui, v *ui.View) error {
	_, cy := v.Cursor()
	_, oy := v.Origin()
	lineNum := cy + oy
	if lineNum+1 >= len(cs.Columns) {
		return nil
	}
	cs.Columns[lineNum], cs.Columns[lineNum+1] = cs.Columns[lineNum+1], cs.Columns[lineNum]
	return cs.CursorDown(g, v)
}

func (cs *ColumnSelector) MoveUp(g *ui.Gui, v *ui.View) error {
	_, cy := v.Cursor()
	_, oy := v.Origin()
	lineNum := cy + oy
	if lineNum <= 0 {
		return nil
	}
	cs.Columns[lineNum], cs.Columns[lineNum-1] = cs.Columns[lineNum-1], cs.Columns[lineNum]
	return cs.CursorUp(g, v)
}

func (cs *ColumnSelector) Edit(v *ui.View, key ui.Key, ch rune, mod ui.Modifier) {
	if '0' <= ch && ch <= '9' {
		if idx, err := strconv.Atoi(string(ch)); err == nil && idx < len(cs.Columns) {
			cs.Columns.ToggleDisplayed(idx)
		}
	} else if key == ui.KeySpace {
		_, cy := v.Cursor()
		_, oy := v.Origin()
		lineNum := cy + oy
		if lineNum < len(cs.Columns) {
			cs.Columns.ToggleDisplayed(lineNum)
		}
	}
}
