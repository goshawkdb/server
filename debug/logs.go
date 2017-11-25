package debug

import (
	"bufio"
	"fmt"
	"github.com/kr/logfmt"
	"io"
	"os"
)

const (
	TS = "ts"
	ML = "multiline"
)

func (r Row) HandleLogfmt(key, val []byte) error {
	r[string(key)] = string(val)
	return nil
}

func LoadRows(r io.Reader) (*Rows, error) {
	scanner := bufio.NewScanner(r)
	rows := make([]Row, 0, 64)
	count := 0
	var prev Row
	for scanner.Scan() {
		line := scanner.Bytes()
		row := make(Row)
		if err := logfmt.Unmarshal(line, row); err != nil {
			return nil, err
		} else if _, found := row[TS]; found {
			row[INDEX] = fmt.Sprintf("%d", count)
			count++
			rows = append(rows, row)
			prev = nil
		} else if prev == nil {
			prev = make(Row)
			prev[ML] = string(line)
			prev[INDEX] = fmt.Sprintf("%d", count)
			count++
			rows = append(rows, prev)
		} else {
			prev[ML] = prev[ML] + "\n" + string(line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return &Rows{All: rows}, nil
}

func RowsFromFile(path string) (*Rows, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	return LoadRows(fd)
}
