package clickhouse

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

type httpBatch struct {
	closed   int32
	conn     *httpConnOpener
	args     [][]driver.NamedValue
	query    string
	location *time.Location
}

func newHttpBatch(c *httpConnOpener, query string, location *time.Location) *httpBatch {
	return &httpBatch{
		conn:     c,
		query:    query,
		args:     make([][]driver.NamedValue, 0),
		location: location,
	}
}

//Exec executes a query that doesn't return rows, such as an INSERT
func (b *httpBatch) Exec(args []driver.Value) (driver.Result, error) {
	//body, err := bind(b.conn.location, b.query, rebind(args)...)
	// TODO
	//return s.exec(context.Background(), args)
	return nil, nil
}

func (b *httpBatch) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	b.args = append(b.args, args)
	return driver.RowsAffected(0), nil
}

// Close closes the statement.
func (b *httpBatch) Close() error {
	if atomic.CompareAndSwapInt32(&b.closed, 0, 1) {
		b.conn = nil
	}
	return nil
}

func (b *httpBatch) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("only Exec method supported in batch mode")
}

// Query executes a query that may return rows, such as a SELECT
func (b *httpBatch) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("only Exec method supported in batch mode")
}

func (b *httpBatch) NumInput() int {
	return -1
}

func (b *httpBatch) commit() error {
	if atomic.CompareAndSwapInt32(&b.closed, 0, 1) {
		conn := b.conn
		b.conn = nil

		location := b.location
		b.location = nil

		args := b.args
		b.args = nil
		if len(args) == 0 {
			return nil
		}

		buf := bytes.NewBufferString(b.query)
		var (
			err error
		)

		for i, arg := range args {
			if i > 0 {
				buf.WriteString(", ")
			}

			formatArgs, err := formatArgs(location, rebind(arg)...)
			if err != nil {
				return fmt.Errorf("error format args %w", err)
			}

			buf.WriteByte('(')
			buf.WriteString(strings.Join(formatArgs, ","))
			buf.WriteByte(')')

		}

		_, err = conn.ExecContext(context.Background(), buf.String(), nil)
		return err
	}
	return nil
}
