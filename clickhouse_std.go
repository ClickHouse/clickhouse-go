package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"sync/atomic"
)

func init() {
	driver := Driver{}
	sql.Register("clickhouse", &driver)
}

type Driver struct {
	counter uint64
}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	atomic.AddUint64(&d.counter, 1)
	return nil, nil
}
