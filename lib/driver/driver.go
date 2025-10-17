package driver

import (
	"context"
	"reflect"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type ServerVersion = proto.ServerHandshake

type (
	NamedValue struct {
		Name  string
		Value any
	}

	NamedDateValue struct {
		Name  string
		Value time.Time
		Scale uint8
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
		Select(ctx context.Context, dest any, query string, args ...any) error
		Query(ctx context.Context, query string, args ...any) (Rows, error)
		QueryRow(ctx context.Context, query string, args ...any) Row
		PrepareBatch(ctx context.Context, query string, opts ...PrepareBatchOption) (Batch, error)
		Exec(ctx context.Context, query string, args ...any) error

		// Deprecated: use context aware `WithAsync()` for any async operations
		AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error
		Ping(context.Context) error
		Stats() Stats
		Close() error
	}
	Row interface {
		Err() error
		Scan(dest ...any) error
		ScanStruct(dest any) error
	}
	Rows interface {
		Next() bool
		Scan(dest ...any) error
		ScanStruct(dest any) error
		ColumnTypes() []ColumnType
		Totals(dest ...any) error
		Columns() []string
		Close() error
		Err() error
	}
	Batch interface {
		Abort() error
		Append(v ...any) error
		AppendStruct(v any) error
		Column(int) BatchColumn
		Flush() error
		Send() error
		IsSent() bool
		Rows() int
		Columns() []column.Interface
		Close() error
	}
	BatchColumn interface {
		Append(any) error
		AppendRow(any) error
	}
	ColumnType interface {
		Name() string
		Nullable() bool
		ScanType() reflect.Type
		DatabaseTypeName() string
	}
)
