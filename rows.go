package clickhouse

import (
	"database/sql/driver"
	"io"
)

type rows struct {
	index   int
	columns []string
	rows    [][]driver.Value
}

func (rows *rows) append(block *block) {
	if len(rows.columns) == 0 && len(block.columnNames) != 0 {
		rows.columns = block.columnNames
	}
	for rowNum := 0; rowNum < int(block.numRows); rowNum++ {
		row := make([]driver.Value, 0, block.numColumns)
		for columnNum := 0; columnNum < int(block.numColumns); columnNum++ {
			row = append(row, block.columns[columnNum][rowNum])
		}
		rows.rows = append(rows.rows, row)
	}
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
