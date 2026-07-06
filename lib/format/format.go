// Package format provides client-side codecs that translate between ClickHouse
// native blocks and named ClickHouse formats (e.g. "CSV", "JSONEachRow").
//
// Codecs are used by the native TCP protocol, where the server always
// exchanges blocks in the Native format and any other format must be produced
// or parsed on the client — mirroring how clickhouse-client implements
// IOutputFormat/IInputFormat. Over the HTTP protocol the server performs the
// conversion itself and no codec is required.
//
// Client-side output is not guaranteed to be byte-identical to server-side
// output of the same format: float rendering, DateTime64 sub-second precision
// and escaping of non-ASCII characters may differ. Values are semantically
// equivalent and round-trip through the matching decoder.
package format

import (
	"io"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// Codec translates between native blocks and a named ClickHouse format on the
// client side. A Codec must be safe for concurrent use; each Encoder or
// Decoder it returns is used by a single goroutine for a single query or
// insert.
type Codec interface {
	// Name returns the canonical, case-sensitive ClickHouse format name, e.g. "CSV".
	Name() string
	// NewEncoder returns an Encoder writing formatted bytes to w (SELECT direction).
	NewEncoder(w io.Writer) Encoder
	// NewDecoder returns a Decoder reading formatted bytes from r (INSERT direction).
	NewDecoder(r io.Reader) Decoder
}

// Encoder writes native blocks as formatted bytes. Encoders are stateful: the
// first WriteBlock may emit a header, Close emits any trailer.
type Encoder interface {
	// WriteBlock encodes block. Zero-row blocks must be accepted: the first
	// block of a result carries only the schema.
	WriteBlock(block *proto.Block) error
	// Close writes any trailer and flushes buffered output.
	// It does not close the underlying writer.
	Close() error
}

// Decoder reads formatted bytes and appends rows to a block whose columns
// were built from the server-provided insert schema.
type Decoder interface {
	// ReadBlock appends up to maxRows rows to block and returns the number of
	// rows appended. It returns io.EOF when the input is exhausted, possibly
	// alongside appended rows.
	ReadBlock(block *proto.Block, maxRows int) (int, error)
}
