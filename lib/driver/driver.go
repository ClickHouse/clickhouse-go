package driver

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type ServerVersion = proto.ServerHandshake

type (
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
		Contributors() []string
		ServerVersion() (*ServerVersion, error)
		Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error
		Query(ctx context.Context, query string, args ...interface{}) (Rows, error)
		QueryRow(ctx context.Context, query string, args ...interface{}) Row
		PrepareBatch(ctx context.Context, query string) (Batch, error)
		Exec(ctx context.Context, query string, args ...interface{}) error
		AsyncInsert(ctx context.Context, query string, wait bool) error
		Ping(context.Context) error
		Stats() Stats
		Close() error
	}
	Row interface {
		Err() error
		Scan(dest ...interface{}) error
		ScanStruct(dest interface{}) error
	}
	Rows interface {
		Next() bool
		Scan(dest ...interface{}) error
		ScanStruct(dest interface{}) error
		Totals(dest ...interface{}) error
		Columns() []string
		Close() error
		Err() error
	}
	Batch interface {
		Append(v ...interface{}) error
		AppendStruct(v interface{}) error
		Column(int) BatchColumn
		Send() error
	}
	BatchColumn interface {
		Append(interface{}) error
	}
)
