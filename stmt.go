package clickhouse

import (
	"bufio"
	"bytes"
	"database/sql/driver"
	"strings"
)

type stmt struct {
	conn     *connect
	query    string
	index    int
	numInput int
}

func (stmt *stmt) NumInput() int {
	if stmt.numInput < 0 {
		return 0
	}
	return stmt.numInput
}

func (stmt *stmt) Exec(args []driver.Value) (driver.Result, error) {
	stmt.conn.log("[stmt] exec. args: %v", args)
	buffer := &bytes.Buffer{}
	if stmt.conn.inTransaction {
		buffer = &stmt.conn.buffers[stmt.index]
	}
	if len(args) != 0 {
		for _, v := range args {
			buffer.WriteString(encode(v) + "\t")
		}
		buffer.WriteString("\n")
	}
	if !stmt.conn.inTransaction {
		if _, err := stmt.conn.do(stmt.query, buffer); err != nil {
			return nil, err
		}
	}
	return &result{}, nil
}

func (stmt *stmt) Query(args []driver.Value) (driver.Rows, error) {
	stmt.conn.log("[stmt] query. args: %v", args)
	var query []string
	for index, value := range strings.Split(stmt.query, "?") {
		query = append(query, value)
		if index < len(args) {
			query = append(query, encode(args[index]))
		}
	}

	body, err := stmt.conn.do(strings.Join(query, ""), &bytes.Buffer{})
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(body)
	scanner.Scan()
	columns := strings.Fields(scanner.Text())
	scanner.Scan()
	types := strings.Fields(scanner.Text())
	stmt.conn.log("[stmt] query. columns: %v, types: %v", columns, types)
	return &rows{
		types:   types,
		scanner: scanner,
		columns: columns,
	}, nil
}

func (stmt *stmt) Close() error {
	stmt.conn.log("[stmt] close")
	return nil
}
