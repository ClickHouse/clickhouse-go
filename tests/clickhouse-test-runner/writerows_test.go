package main

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fakeRows gives the UInt64 values of vals as rows, then fails with err.
type fakeRows struct {
	vals []uint64
	i    int
	err  error
}

type fakeColumnType struct{}

func (fakeColumnType) Name() string             { return "x" }
func (fakeColumnType) Nullable() bool           { return false }
func (fakeColumnType) ScanType() reflect.Type   { return reflect.TypeOf(uint64(0)) }
func (fakeColumnType) DatabaseTypeName() string { return "UInt64" }

func (f *fakeRows) Next() bool {
	if f.i >= len(f.vals) {
		return false
	}
	f.i++
	return true
}

func (f *fakeRows) Scan(dest ...any) error {
	*dest[0].(*uint64) = f.vals[f.i-1]
	return nil
}

func (f *fakeRows) ScanStruct(any) error             { return errors.New("not used") }
func (f *fakeRows) ColumnTypes() []driver.ColumnType { return []driver.ColumnType{fakeColumnType{}} }
func (f *fakeRows) Totals(...any) error              { return sql.ErrNoRows }
func (f *fakeRows) Columns() []string                { return []string{"x"} }
func (f *fakeRows) Close() error                     { return nil }
func (f *fakeRows) Err() error                       { return f.err }
func (f *fakeRows) HasData() bool                    { return len(f.vals) > 0 }

// TestWriteRowsBeforeStreamError: the rows received before an exception in a
// later block are written, as clickhouse-client prints them.
func TestWriteRowsBeforeStreamError(t *testing.T) {
	streamErr := errors.New("exception in a later block")
	r := &runner{}
	err := r.writeRows(&fakeRows{vals: []uint64{0, 1, 2}, err: streamErr}, outputFormat{Name: "TabSeparated", Supported: true, Names: true})
	require.ErrorIs(t, err, streamErr)
	assert.Equal(t, "x\n0\n1\n2\n", r.result.String())
}

func TestWriteRowsAllRows(t *testing.T) {
	r := &runner{}
	require.NoError(t, r.writeRows(&fakeRows{vals: []uint64{7, 8}}, outputFormat{Name: "TabSeparated", Supported: true}))
	assert.Equal(t, "7\n8\n", r.result.String())
}
