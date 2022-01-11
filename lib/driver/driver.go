package driver

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type ServerVersion = proto.ServerHandshake

type (
	Block interface {
		Rows() int
	}
	NamedValue struct {
		Name  string
		Value interface{}
	}
	Stats struct {
		MaxOpenConns int
		MaxIdleConns int
		Open         int
		Idle         int
	}
)

type (
	Conn interface {
		ServerVersion() (*ServerVersion, error)
		// Get(tx context.Context, dst interface{}, query string, args ...interface{}) error
		// Select(tx context.Context, dst interface{}, query string, args ...interface{}) error
		Query(ctx context.Context, query string, args ...interface{}) (Rows, error)
		QueryRow(ctx context.Context, query string, args ...interface{}) Row
		PrepareBatch(ctx context.Context, query string) (Batch, error)
		Exec(ctx context.Context, query string, args ...interface{}) error
		Ping(context.Context) error
		Stats() Stats
		Close() error
	}
	Row interface {
		Scan(dest ...interface{}) error
		Err() error
	}
	Rows interface {
		Next() bool
		Scan(dest ...interface{}) error
		Columns() []string
		Close() error
		Err() error
		// Totals(dest ...interface{}) error
		// Extremes(dest ...interface{}) error
	}
	Batch interface {
		Append(v ...interface{}) error
		Column(int) BatchColumn
		Send() error
	}
	BatchColumn interface {
		Append(interface{}) error
	}
)
