package debug

import (
	"fmt"
	ui "github.com/jroimartin/gocui"
	"io"
	"sort"
	"strings"
)

const (
	INDEX        = "53c6cfb4-d284-457e-8f6b-fe6b0cfb8203"
	TAGGED       = "t"
	TAGGED_VALUE = "*"
)

type Row map[string]string

func (r Row) Format(w io.Writer, rs *Rows, cs Columns, highlight bool) {
	for _, c := range cs {
		if !c.Displayed {
			continue
		}
		orig := r[c.Name]
		orig = strings.Replace(string(orig), "\n", "↵ ", -1)
		val := orig
		diff := (c.Width - 1) - len(val)
		if diff < 0 { // val too long
			val = val[c.Width-1:]
		} else if diff > 0 {
			val += strings.Repeat(" ", diff)
		}
		if highlight && c.Selected {
			fmt.Fprintf(w, "\033[7m%s\033[0m ", val)
		} else if len(rs.MatchingKey) != 0 && c.Name == rs.MatchingKey && orig == rs.MatchingValue {
			fmt.Fprintf(w, "\033[1m%s\033[0m ", val)
		} else {
			fmt.Fprintf(w, "%s ", val)
		}
	}
	fmt.Fprintln(w)
}

type Rows struct {
	All           []Row
	Selected      []Row
	MatchingKey   string
	MatchingValue string
	TaggedCount   int
}

// this gets all keys, sorts them by decreasing frequency, and then by
// ascending name. It may not be perfect, but it's at least
// deterministic.
func (rs *Rows) AllColumns() Columns {
	freqs := make(map[string]*Column)
	for _, row := range rs.All {
		for key, val := range row {
			if key == INDEX {
				continue
			}
			col, found := freqs[key]
			if !found {
				col = &Column{
					Name:      key,
					Displayed: true,
					Width:     len(key) + 1,
				}
				freqs[key] = col
			}
			col.XStart++ // we abuse this field as a freq counter
			if len(val)+1 > col.Width {
				col.Width = len(val) + 1
			}
		}
	}
	invertedFreqs := make(map[int]*[]string)
	for key, col := range freqs {
		freq := col.XStart
		if keys, found := invertedFreqs[freq]; found {
			*keys = append(*keys, key)
		} else {
			keys := []string{key}
			invertedFreqs[freq] = &keys
		}
	}
	uniqFreqs := make([]int, 0, len(invertedFreqs))
	for freq := range invertedFreqs {
		uniqFreqs = append(uniqFreqs, freq)
	}
	sort.Slice(uniqFreqs, func(i, j int) bool { return uniqFreqs[i] > uniqFreqs[j] })
	cols := make(Columns, 1, len(freqs)+1)
	cols[0] = &Column{
		Name:      TAGGED,
		Displayed: true,
		Width:     2,
		Selected:  true,
	}
	for _, freq := range uniqFreqs {
		keys := *(invertedFreqs[freq])
		sort.Strings(keys)
		for _, key := range keys {
			col := freqs[key]
			col.XStart = 0 // just reset that
			cols = append(cols, col)
		}
	}
	return cols
}

func (rs *Rows) Format(w io.Writer, cs Columns, from, count, highlight int) {
	selected := rs.Selected
	if from > len(selected) || from < 0 {
		return
	}
	selected = selected[from:]
	if count < 0 {
		return
	}
	if count < len(selected) {
		selected = selected[:count]
	}
	for idx, r := range selected {
		r.Format(w, rs, cs, idx+from == highlight)
	}
}

func (rs *Rows) SelectAll() {
	rs.Selected = make([]Row, len(rs.All))
	copy(rs.Selected, rs.All)
}

func (rs *Rows) LimitSelected(key, value string) {
	selected := make([]Row, 0, len(rs.Selected))
	for _, r := range rs.Selected {
		if e := r[key]; e == value {
			selected = append(selected, r)
		}
	}
	rs.Selected = selected
}

func (rs *Rows) SetMatch(key, value string) {
	rs.MatchingKey = key
	rs.MatchingValue = value
}

func (rs *Rows) NextMatch(from int, forwards bool) int {
	if len(rs.MatchingKey) == 0 {
		return from
	}
	if forwards {
		for idx := from + 1; idx < len(rs.Selected); idx++ {
			r := rs.Selected[idx]
			if e := r[rs.MatchingKey]; e == rs.MatchingValue {
				return idx
			}
		}
	} else {
		for idx := from - 1; idx >= 0; idx-- {
			r := rs.Selected[idx]
			if e := r[rs.MatchingKey]; e == rs.MatchingValue {
				return idx
			}
		}
	}
	return from
}

func (rs *Rows) Values(key string) []string {
	values := make(map[string]int)
	for _, row := range rs.Selected {
		if v, found := row[key]; found {
			v = strings.Replace(string(v), "\n", "↵ ", -1)
			if _, found := values[v]; !found {
				values[v] = len(values)
			}
		}
	}
	result := make([]string, len(values))
	for v, idx := range values {
		result[idx] = v
	}
	return result
}

func (rs *Rows) TagToggle(row Row) {
	if _, tagged := row[TAGGED]; tagged {
		delete(row, TAGGED)
		rs.TaggedCount--
	} else {
		row[TAGGED] = TAGGED_VALUE
		rs.TaggedCount++
	}
}

func (rs *Rows) TagLimit() {
	if rs.TaggedCount > 0 {
		rs.SelectAll()
		rs.LimitSelected(TAGGED, TAGGED_VALUE)
	}
}

func (rs *Rows) TagNone() {
	if rs.TaggedCount > 0 {
		rs.TaggedCount = 0
		for _, row := range rs.All {
			delete(row, TAGGED)
		}
	}
}

type RowsGui struct {
	*Rows
	*DebugGui
	from      int
	highlight int
}

func (rg *RowsGui) Layout(g *ui.Gui) error {
	screenWidth, screenHeight := g.Size()
	infoHeight := 0
	if rg.InfoPanel.Displayed {
		ip, err := g.View(INFO)
		if err != nil && err != ui.ErrUnknownView {
			return err
		}
		if err == nil {
			_, infoHeight = ip.Size()
			infoHeight += 1
		}
	}
	top := HEADERS_HEIGHT - 1
	bottom := screenHeight - EVENTS_HEIGHT - infoHeight
	if bottom <= top {
		return nil
	}
	v, err := g.SetView(ROWS, 0, top, screenWidth-1, bottom)
	if err != nil {
		if err != ui.ErrUnknownView {
			return err
		}
		v.Frame = false
	}
	v.Clear()
	height := bottom - top
	rg.Format(v, rg.Columns, rg.from, height, rg.highlight)

	headers, err := g.View(HEADERS)
	if err != nil {
		return err
	}
	ox, _ := headers.Origin()
	return v.SetOrigin(ox, 0)
}

func (rg *RowsGui) SetHighlight(h int, g *ui.Gui) error {
	if h < 0 {
		h = 0
	} else if h >= len(rg.Selected) {
		h = len(rg.Selected) - 1
	}
	v, err := g.View(ROWS)
	if err != nil {
		return err
	}
	rg.highlight = h
	// we've changed the highlighted row, so if the info panel is on,
	// we should get it to relayout itself, which will cause us to
	// relayout ourself, which will mean the next call to v.Size() will
	// give us the right height.
	if err = rg.InfoPanel.Layout(g); err != nil {
		return err
	}
	_, height := v.Size()
	to := height + rg.from
	if rg.highlight >= to {
		rg.from = rg.highlight - height + 1
	} else if rg.highlight < rg.from {
		rg.from = rg.highlight
	}
	return nil
}

func (rg *RowsGui) Down(g *ui.Gui, v *ui.View) error {
	return rg.SetHighlight(rg.highlight+1, g)
}

func (rg *RowsGui) Up(g *ui.Gui, v *ui.View) error {
	return rg.SetHighlight(rg.highlight-1, g)
}

func (rg *RowsGui) Top(g *ui.Gui, v *ui.View) error {
	return rg.SetHighlight(0, g)
}

func (rg *RowsGui) Bottom(g *ui.Gui, v *ui.View) error {
	return rg.SetHighlight(len(rg.Selected)-1, g)
}

func (rg *RowsGui) PageDown(g *ui.Gui, v *ui.View) error {
	v, err := g.View(ROWS)
	if err != nil {
		return err
	}
	_, height := v.Size()
	height--
	height /= 2
	if height == 0 {
		height = 1
	}
	return rg.SetHighlight(rg.highlight+height, g)
}

func (rg *RowsGui) PageUp(g *ui.Gui, v *ui.View) error {
	v, err := g.View(ROWS)
	if err != nil {
		return err
	}
	_, height := v.Size()
	height--
	height /= 2
	if height == 0 {
		height = 1
	}
	return rg.SetHighlight(rg.highlight-height, g)
}

func (rg *RowsGui) Limit(g *ui.Gui, v *ui.View) error {
	key := ""
	for _, c := range rg.Columns {
		if c.Selected {
			key = c.Name
			break
		}
	}
	row := rg.Selected[rg.highlight]
	val, found := row[key]
	if found && len(val) > 0 {
		screenRow := rg.highlight - rg.from
		rg.LimitSelected(key, val)
		// we want to try to keep the current row in the same place on
		// the screen.
		for idx, r := range rg.Selected {
			if r[INDEX] == row[INDEX] {
				if screenRow > idx {
					screenRow = idx
				}
				rg.from = idx - screenRow
				rg.highlight = idx
				break
			}
		}
		return AppendEvent(g, fmt.Sprintf("Added constraint %s=%s. %d matching rows", key, val, len(rg.Selected)))
	}
	return nil
}

func (rg *RowsGui) All(g *ui.Gui, v *ui.View) error {
	v, err := g.View(ROWS)
	if err != nil {
		return err
	}
	_, height := v.Size()
	// again, we try to keep the same row in the same place on the screen
	row := rg.Selected[rg.highlight]
	screenRow := rg.highlight - rg.from
	screenRowBotUp := height - screenRow
	rg.SelectAll()
	for idx, r := range rg.Selected {
		if r[INDEX] == row[INDEX] {
			if screenRowBotUp >= len(rg.Selected)-idx {
				screenRow = height - (len(rg.Selected) - idx)
			}
			rg.highlight = idx
			rg.from = idx - screenRow
			break
		}
	}
	return AppendEvent(g, fmt.Sprintf("Removed all constraints. %d rows.", len(rg.Selected)))
}

func (rg *RowsGui) TagToggle(g *ui.Gui, v *ui.View) error {
	rg.Rows.TagToggle(rg.Selected[rg.highlight])
	return rg.Down(g, v)
}

func (rg *RowsGui) TagNone(g *ui.Gui, v *ui.View) error {
	rg.Rows.TagNone()
	return AppendEvent(g, "Cleared all tags.")
}

func (rg *RowsGui) TagLimit(g *ui.Gui, v *ui.View) error {
	if rg.TaggedCount == 0 {
		return nil
	}
	rg.Rows.TagLimit()
	if err := rg.SetHighlight(0, g); err != nil {
		return nil
	}
	return AppendEvent(g, fmt.Sprintf("Limiting to tagged rows. %d rows", len(rg.Selected)))
}

func (rg *RowsGui) SearchNext(g *ui.Gui, v *ui.View) error {
	if len(rg.MatchingKey) == 0 {
		key := ""
		for _, c := range rg.Columns {
			if c.Selected {
				key = c.Name
				break
			}
		}
		row := rg.Selected[rg.highlight]
		val, found := row[key]
		if found && len(val) > 0 {
			rg.SetMatch(key, val)
			return AppendEvent(g, fmt.Sprintf("Highlighting %s=%s.", key, val))
		}
		return nil
	} else {
		v, err := g.View(ROWS)
		if err != nil {
			return err
		}
		_, height := v.Size()
		old := rg.highlight
		rg.highlight = rg.NextMatch(rg.highlight, true)
		if rg.highlight >= rg.from+height {
			// we should re-center the screen
			rg.from = rg.highlight - (height / 2)
			if rg.from+height > len(rg.Selected) {
				rg.from = len(rg.Selected) - height
			}
		}
		if old == rg.highlight {
			return AppendEvent(g, fmt.Sprintf("No further matches found."))
		}
		return nil
	}
}

func (rg *RowsGui) SearchPrev(g *ui.Gui, v *ui.View) error {
	if len(rg.MatchingKey) == 0 {
		key := ""
		for _, c := range rg.Columns {
			if c.Selected {
				key = c.Name
				break
			}
		}
		row := rg.Selected[rg.highlight]
		val, found := row[key]
		if found && len(val) > 0 {
			rg.SetMatch(key, val)
			return AppendEvent(g, fmt.Sprintf("Highlighting %s=%s.", key, val))
		}
		return nil
	} else {
		v, err := g.View(ROWS)
		if err != nil {
			return err
		}
		_, height := v.Size()
		old := rg.highlight
		rg.highlight = rg.NextMatch(rg.highlight, false)
		if rg.highlight < rg.from {
			// we should re-center the screen
			rg.from = rg.highlight - (height / 2)
			if rg.from < 0 {
				rg.from = 0
			}
		}
		if old == rg.highlight {
			return AppendEvent(g, fmt.Sprintf("No further matches found."))
		}
		return nil
	}
}

func (rg *RowsGui) StopSearch(g *ui.Gui, v *ui.View) error {
	rg.SetMatch("", "")
	return AppendEvent(g, fmt.Sprintf("Cleared Highlighting."))
}
