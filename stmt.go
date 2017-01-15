package clickhouse

import "database/sql/driver"

type stmt struct {
	numInput int
}

func (stmt *stmt) NumInput() int {
	if stmt.numInput < 0 {
		return 0
	}
	return stmt.numInput
}

func (stmt *stmt) Exec(args []driver.Value) (driver.Result, error) {

	return &result{}, nil
}

func (stmt *stmt) Query(args []driver.Value) (driver.Rows, error) {

	return &rows{}, nil
}

func (stmt *stmt) Close() error {

	return nil
}
