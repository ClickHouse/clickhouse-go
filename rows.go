package clickhouse

import "database/sql/driver"
import "io"

type rows struct {
	index   int
	columns []string
	rows    [][]driver.Value
}

func (rows *rows) append(d *datapacket) {
	if len(rows.columns) == 0 && len(d.columns) != 0 {
		rows.columns = d.columns
	}
	rows.rows = append(rows.rows, d.rows...)
}

func (rows *rows) Columns() []string {
	return rows.columns
}

func (rows *rows) Next(dest []driver.Value) error {
	if len(rows.rows) <= rows.index {
		return io.EOF
	}
	for i := range dest {
		dest[i] = rows.rows[rows.index][i]
	}
	rows.index++
	return nil
}

func (rows *rows) Close() error {
	rows.rows = rows.rows[0:0]
	rows.columns = rows.columns[0:0]
	return nil
}
