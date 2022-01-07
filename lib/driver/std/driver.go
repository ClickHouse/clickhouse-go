package std

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("clickhouse", &Driver{})
}

type Driver struct {
	//conn ch.
}

func (Driver) Open(dsn string) (driver.Conn, error) {
	return &Driver{}, nil
}

func (d *Driver) Begin() (driver.Tx, error) {
	return d, nil
}

func (d *Driver) Commit() error {
	return nil
}

func (d *Driver) Rollback() error {
	return nil
}

func (d *Driver) Close() error {
	return nil
}

func (d *Driver) Prepare(query string) (driver.Stmt, error) {
	return &stmt{}, nil
}

type stmt struct{}

func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return &rows{}, nil
}
func (s *stmt) Close() error {
	return nil
}

type rows struct{}

func (*rows) Columns() []string {
	return nil
}
func (*rows) Next(dest []driver.Value) error {
	// io.EOF
	return nil
}

func (r *rows) Close() error {
	return nil
}
