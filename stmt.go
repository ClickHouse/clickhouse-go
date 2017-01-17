package clickhouse

import (
	"database/sql/driver"
	"strings"
)

type stmt struct {
	ch         *clickhouse
	rows       rows
	query      string
	numInput   int
	isInsert   bool
	datapacket *datapacket
}

func (stmt *stmt) NumInput() int {
	if stmt.numInput < 0 {
		return 0
	}
	return stmt.numInput
}

func (stmt *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.isInsert {
		if err := stmt.ch.batch.insert(args); err != nil {
			return nil, err
		}
		return &result{}, nil
	}
	if err := stmt.ch.sendQuery(stmt.query); err != nil {
		return nil, err
	}
	if _, err := stmt.ch.receivePacket(); err != nil {
		return nil, err
	}
	return &result{}, nil
}

func (stmt *stmt) Query(args []driver.Value) (driver.Rows, error) {
	var query []string
	for index, value := range strings.Split(stmt.query, "?") {
		query = append(query, value)
		if index < len(args) {
			query = append(query, quote(args[index]))
		}
	}
	if err := stmt.ch.sendQuery(strings.Join(query, "")); err != nil {
		return nil, err
	}
	rows, err := stmt.ch.receivePacket()
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (stmt *stmt) Close() error {
	stmt.ch.log("[stmt] close")
	stmt.rows = rows{}
	return nil
}
