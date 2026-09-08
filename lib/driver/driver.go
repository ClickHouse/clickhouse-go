package driver

import (
	"context"
	"io"
	"reflect"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type ServerVersion = proto.ServerHandshake

type (
	// NamedValue is a query argument with a name. Create it with
	// clickhouse.Named.
	NamedValue struct {
		Name  string
		Value any
	}

	// NamedDateValue is a time query argument with a name and an explicit
	// precision. Create it with clickhouse.DateNamed. Scale holds a
	// clickhouse.TimeUnit: 0 seconds, 1 milli, 2 micro, 3 nano.
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
	// Conn is the client-facing connection interface of the native API,
	// obtained from clickhouse.Open.
	//
	// Compatibility: Conn is meant to be consumed, not implemented. It is
	// NOT backward compatible for implementers - new methods are added in
	// minor releases as driver and server features land, which is a compile
	// break for any type that hand-implements it (decorators, adapters,
	// test fakes). Embed the interface in such types instead of implementing
	// every method, so added methods do not break the build:
	//
	//	type loggingConn struct {
	//		driver.Conn // forwards everything not overridden
	//	}
	Conn interface {
		Contributors() []string
		ServerVersion() (*ServerVersion, error)
		Select(ctx context.Context, dest any, query string, args ...any) error
		Query(ctx context.Context, query string, args ...any) (Rows, error)
		QueryRow(ctx context.Context, query string, args ...any) Row
		PrepareBatch(ctx context.Context, query string, opts ...PrepareBatchOption) (Batch, error)
		Exec(ctx context.Context, query string, args ...any) error

		// QueryFormat executes query and returns the result encoded in the
		// given ClickHouse format (e.g. "CSV", "JSONEachRow", "Parquet") as a raw
		// byte stream. The caller must Close the returned stream; until then it
		// holds a connection (visible in Stats). Put the format in the format
		// argument, not as a FORMAT clause in the query. The stream carries
		// the raw format bytes: transport compression (Options.Compression)
		// is transparent and already decoded.
		//
		// The stream is not safe for concurrent use: to abort a stalled Read,
		// cancel the query context rather than calling Close from another
		// goroutine. Closing before EOF may discard the underlying HTTP
		// connection instead of returning it for reuse.
		//
		// If the query fails after streaming has begun, Read returns the data
		// received so far followed by the server exception. Exception blocks
		// are identified by the per-response random tag the server announces
		// in the X-ClickHouse-Exception-Tag header, so result data that
		// happens to contain the marker bytes is served verbatim. Only on
		// older servers that send no tag header does detection fall back to
		// best-effort marker scanning, which can misdetect such data; on
		// those servers a compressed response carries no exception block at
		// all, and a mid-stream failure surfaces as the decompressor's
		// truncation error instead of the server's message. For
		// all-or-nothing semantics set wait_end_of_query=1 (via WithSettings
		// or connection-level Options.Settings): the server then buffers the
		// complete result, failures surface as an error from QueryFormat
		// itself, and the stream is served unscanned.
		//
		// So that failures always arrive as such a block regardless of
		// format, http_write_exception_in_output_format is pinned to 0 for
		// the query - with it, formats that can carry an error in-band (JSON,
		// XML, ...) would embed it in a valid payload and end the stream
		// cleanly. An explicit caller setting takes precedence over the pin.
		//
		// Experimental: this API is experimental and may change or be removed
		// in a future minor release. It is currently only supported over the
		// HTTP protocol, where the server encodes the stream and every
		// server-supported format works; over the native protocol it returns
		// clickhouse.ErrFormatNativeUnsupported.
		QueryFormat(ctx context.Context, format string, query string, args ...any) (io.ReadCloser, error)

		// InsertFormat executes the INSERT statement query, streaming
		// data (pre-encoded in the given format) as the insert payload. Any FORMAT
		// clause or VALUES suffix in query is replaced; the format argument is
		// authoritative. It returns once the server has committed or rejected the
		// insert.
		//
		// data must be the raw, uncompressed format bytes (e.g. a plain
		// Parquet file). Transport compression is transparent: with
		// Options.Compression set, the driver itself compresses the payload
		// on the wire and sets Content-Encoding. Passing pre-compressed data
		// (such as a .parquet.gz file) therefore compresses it twice, and the
		// server rejects the once-decoded payload as malformed format data.
		//
		// Experimental: this API is experimental and may change or be removed
		// in a future minor release. It is currently only supported over the
		// HTTP protocol, where the server parses the payload and every
		// server-supported format works; over the native protocol it returns
		// clickhouse.ErrFormatNativeUnsupported.
		InsertFormat(ctx context.Context, format string, query string, data io.Reader) error

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
		HasData() bool
	}

	// Batch represents a prepared INSERT that buffers rows client-side and sends them to ClickHouse.
	//
	// Typical usage:
	//
	//	batch, err := conn.PrepareBatch(ctx, "INSERT INTO t")
	//	if err != nil { ... }
	//	defer batch.Close() // cleanup if Send is not reached
	//
	//	for ... {
	//		_ = batch.Append(...)
	//		// Optionally flush periodically for native protocol.
	//		// _ = batch.Flush()
	//	}
	//	_ = batch.Send()
	//
	// Notes:
	// - After Send(), the batch is considered finalized (IsSent() becomes true). Create a new batch to send more rows.
	// - For HTTP protocol, Flush() is currently a no-op. Use Send() to transmit buffered rows.
	Batch interface {
		Abort() error
		Append(v ...any) error
		AppendStruct(v any) error
		Column(int) BatchColumn

		// Flush sends the currently buffered rows but keeps the batch usable.
		//
		// For native protocol this transmits the buffered block to the server and clears the local buffer.
		// For HTTP protocol this is currently a no-op.
		Flush() error

		// Send flushes any buffered rows and finalizes the INSERT.
		// After Send() the batch is considered sent and should not be reused.
		Send() error

		// IsSent reports whether the batch has been finalized via Send(), Abort(), or Close().
		IsSent() bool
		Rows() int
		Columns() []column.Interface

		// Close ends the current INSERT and releases resources.
		//
		// It is safe (and recommended) to call Close via defer immediately after PrepareBatch.
		// Close does not guarantee that buffered rows are sent; call Send() to finalize the INSERT.
		Close() error
	}
	BatchColumn interface {
		// Append appends a value to the underlying column buffer.
		Append(any) error
		// AppendRow appends a row-oriented value to the underlying column buffer.
		AppendRow(any) error
	}
	ColumnType interface {
		Name() string
		Nullable() bool
		ScanType() reflect.Type
		DatabaseTypeName() string
	}
)
