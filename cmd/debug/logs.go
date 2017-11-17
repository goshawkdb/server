package main

import (
	"bufio"
	"github.com/kr/logfmt"
	"io"
	"os"
	"strings"
)

const (
	TS = "ts"
	ML = "multiline"
)

func (r Row) HandleLogfmt(key, val []byte) error {
	strVal := strings.Replace(string(val), "\n", "↵ ", -1)
	r[string(key)] = strVal
	return nil
}

func LoadRows(r io.Reader) (*Rows, error) {
	scanner := bufio.NewScanner(r)
	rows := make([]Row, 0, 64)
	var prev Row
	for scanner.Scan() {
		line := scanner.Bytes()
		row := make(Row)
		if err := logfmt.Unmarshal(line, row); err != nil {
			return nil, err
		} else if _, found := row[TS]; found {
			rows = append(rows, row)
			prev = nil
		} else if prev == nil {
			row = make(Row)
			row[ML] = string(line)
			rows = append(rows, row)
			prev = row
		} else {
			prev[ML] = prev[ML] + "↵ " + string(line)
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
