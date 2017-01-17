package clickhouse

import "database/sql/driver"

type batch struct {
	datapacket *datapacket
}

func (batch *batch) insert(args []driver.Value) error {
	return nil
}
