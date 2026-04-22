package driver

// InsertFormat specifies the data format used for batch INSERT operations.
// By default, the driver uses FORMAT Native (binary columnar blocks), which is efficient
// but incompatible with ClickHouse's async_insert buffering.
// Text-based formats like JSONEachRow are compatible with async_insert.
type InsertFormat string

const (
	// InsertFormatNative uses FORMAT Native (binary columnar blocks).
	// This is the default and most efficient format, but it bypasses async_insert buffering.
	InsertFormatNative InsertFormat = ""

	// InsertFormatJSONEachRow uses FORMAT JSONEachRow (newline-delimited JSON objects).
	// This format is compatible with ClickHouse async_insert buffering, allowing the server
	// to batch multiple small inserts into larger parts, reducing merge pressure.
	// Only supported with the HTTP protocol. Native protocol (TCP) always uses FORMAT Native.
	InsertFormatJSONEachRow InsertFormat = "JSONEachRow"
)

type PrepareBatchOptions struct {
	ReleaseConnection bool
	CloseOnFlush      bool
	// InsertFormat overrides the default insert format for this batch.
	// If empty, uses the connection-level default (Options.InsertFormat).
	InsertFormat InsertFormat
}

type PrepareBatchOption func(options *PrepareBatchOptions)

// WithReleaseConnection releases the underlying connection back to the pool immediately after PrepareBatch.
//
// This is useful for long-lived batches that should not hold a connection open between Flush/Send calls.
// The driver will reacquire a connection when it needs to transmit data.
func WithReleaseConnection() PrepareBatchOption {
	return func(options *PrepareBatchOptions) {
		options.ReleaseConnection = true
	}
}

// WithCloseOnFlush closes the current INSERT and releases the connection whenever Flush is executed.
//
// This can be used to send data incrementally without keeping a server-side INSERT open.
func WithCloseOnFlush() PrepareBatchOption {
	return func(options *PrepareBatchOptions) {
		options.CloseOnFlush = true
	}
}

// WithInsertFormat sets the data format for this batch INSERT operation.
// Use InsertFormatJSONEachRow to enable compatibility with ClickHouse async_insert buffering.
// Only supported with the HTTP protocol; native protocol (TCP) always uses FORMAT Native.
func WithInsertFormat(format InsertFormat) PrepareBatchOption {
	return func(options *PrepareBatchOptions) {
		options.InsertFormat = format
	}
}
