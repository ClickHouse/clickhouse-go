package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/text"
	"io"
	"reflect"
)

type colReader interface {
	Read() ([]string, error)
}

func newTextRows(c *httpConnOpener, body io.ReadCloser) (*textRows, error) {
	tsvReader := newReader(body)

	colNames, err := tsvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("newTextRows: failed to parse the list of columns: %w", err)
	}

	columnTypes, err := tsvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("newTextRows: failed to parse the list of column types: %w", err)
	}

	types := make([]text.Interface, 0)
	for _, v := range columnTypes {
		colType, err := text.Type(v).ColumnType()
		if err != nil {
			return nil, err
		}
		types = append(types, colType)
	}

	return &textRows{
		c:        c,
		respBody: body,
		tsv:      tsvReader,
		columns:  types,
		colNames: colNames,
	}, nil
}

type textRows struct {
	c        *httpConnOpener
	respBody io.ReadCloser
	tsv      colReader
	colNames []string
	columns  []text.Interface
}

func (r *textRows) Columns() []string {
	return r.colNames
}

func (r *textRows) Close() error {
	return r.respBody.Close()
}

func (r *textRows) Next(dest []driver.Value) error {
	if len(r.columns) != len(dest) {
		return &OpError{
			Op:  "Next",
			Err: fmt.Errorf("expected %d destination arguments in Next, not %d", len(r.columns), len(dest)),
		}
	}

	row, err := r.tsv.Read()
	if err != nil {
		return err
	}

	if len(row) == 1 && row[0] == "" {
		row, err = r.tsv.Read()
		if err != nil {
			return err
		}
	}

	for i, s := range row {
		v, err := r.columns[i].Decode(s)
		if err != nil {
			return err
		}
		dest[i] = v
	}

	return nil
}

// ColumnTypeScanType implements the driver.RowsColumnTypeScanType
func (r *textRows) ColumnTypeScanType(index int) reflect.Type {
	return r.columns[index].Type()
}

// TODO add support ColumnTypeNullable(idx int) (nullable, ok bool)
//func (r *textRows) ColumnTypeNullable(idx int) (nullable, ok bool) {
//	//_, ok = r.columns[idx].(*column.Nullable)
//	return ok, true
//}

// TODO add support ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeDatabaseTypeName
//func (r *textRows) ColumnTypeDatabaseTypeName(index int) string {
//	return "cast type"
//	return r.columns[index]
//}
