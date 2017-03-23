package clickhouse

import (
	"context"
	"database/sql/driver"
	"strings"
)

type stmt struct {
	ch       *clickhouse
	query    string
	numInput int
	isInsert bool
}

var emptyResult = &result{}

func (stmt *stmt) NumInput() int {
	if stmt.numInput < 0 {
		return 0
	}
	return stmt.numInput
}

func (stmt *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return stmt.execContext(context.Background(), args)
}

func (stmt *stmt) execContext(ctx context.Context, args []driver.Value) (driver.Result, error) {
	if stmt.isInsert {
		if err := stmt.ch.data.append(args); err != nil {
			return nil, err
		}
		return emptyResult, nil
	}
	if err := stmt.ch.sendQuery(stmt.query); err != nil {
		return nil, err
	}
	if _, err := stmt.ch.receiveData(); err != nil {
		return nil, err
	}
	return emptyResult, nil
}

func (stmt *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.queryContext(context.Background(), convertOldArgs(args))
}

func (stmt *stmt) queryContext(ctx context.Context, args []namedValue) (driver.Rows, error) {
	var (
		sql   = stmt.query
		query []string
	)
	for _, v := range args {
		if len(v.Name) != 0 {
			sql = strings.Replace(sql, "@"+v.Name, quote(v.Value), -1)
		}
	}
	for index, value := range strings.Split(sql, "?") {
		query = append(query, value)
		if index < len(args) && len(args[index].Name) == 0 {
			query = append(query, quote(args[index].Value))
		}
	}
	if err := stmt.ch.sendQuery(strings.Join(query, "")); err != nil {
		return nil, err
	}
	rows, err := stmt.ch.receiveData()
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (stmt *stmt) Close() error {
	stmt.ch.log("[stmt] close")
	return nil
}

type namedValue struct {
	Name    string
	Ordinal int
	Value   driver.Value
}

func convertOldArgs(args []driver.Value) []namedValue {
	dargs := make([]namedValue, len(args))
	for i, v := range args {
		dargs[i] = namedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return dargs
}
