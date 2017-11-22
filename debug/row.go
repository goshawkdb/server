package debug

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

type Row map[string]string

func (r Row) Format(w io.Writer, rs *Rows, cs Columns, highlight bool) {
	for _, c := range cs {
		if !c.Displayed {
			continue
		}
		orig := r[c.Name]
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
}

// this gets all keys, sorts them by decreasing frequency, and then by
// ascending name. It may not be perfect, but it's at least
// deterministic.
func (rs *Rows) AllColumns() Columns {
	freqs := make(map[string]*Column)
	for _, row := range rs.All {
		for key, val := range row {
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
	cols := make(Columns, 0, len(freqs))
	for _, freq := range uniqFreqs {
		keys := *(invertedFreqs[freq])
		sort.Strings(keys)
		for _, key := range keys {
			col := freqs[key]
			col.XStart = 0 // just reset that
			cols = append(cols, col)
		}
	}
	if len(cols) > 0 {
		cols[0].Selected = true
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
