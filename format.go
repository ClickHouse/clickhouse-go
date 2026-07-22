package clickhouse

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"regexp"
)

// formatNameMatch validates a ClickHouse format name. The name is inserted
// into the query text (INSERT ... FORMAT <name>), so anything beyond a plain
// identifier is rejected before it can reach the server as SQL.
var formatNameMatch = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`)

func validateFormatName(format string) error {
	if !formatNameMatch.MatchString(format) {
		return fmt.Errorf("clickhouse: invalid format name %q: must be a plain identifier such as CSV, JSONEachRow or Parquet", format)
	}
	return nil
}

// trailingFormatClause matches a FORMAT clause at the very end of a query,
// e.g. "SELECT 1 FORMAT JSONEachRow". Anchored at end and requiring whitespace
// before FORMAT so it does not fire on the token appearing inside a string
// literal or identifier earlier in the query.
var trailingFormatClause = regexp.MustCompile(`(?is)\sFORMAT\s+[A-Za-z][A-Za-z0-9]*\s*;?\s*$`)

// QueryFormat executes query and returns the result encoded in the given
// ClickHouse format as a raw byte stream. See driver.Conn for the full
// contract.
//
// The format is passed as the argument, not written into the query: a query
// that carries its own trailing FORMAT clause is rejected, because the server
// would honour that clause over the requested format and silently return a
// different encoding than asked for.
//
// Experimental: this API is experimental and may change or be removed in a
// future minor release. It is currently only supported over the HTTP
// protocol; over the native protocol it returns ErrFormatNativeUnsupported.
func (ch *clickhouse) QueryFormat(ctx context.Context, format string, query string, args ...any) (io.ReadCloser, error) {
	if err := validateFormatName(format); err != nil {
		return nil, err
	}
	if trailingFormatClause.MatchString(query) {
		return nil, fmt.Errorf("clickhouse: query must not contain a trailing FORMAT clause; pass the format as the QueryFormat argument (%q) instead", format)
	}
	conn, err := ch.acquire(ctx)
	if err != nil {
		return nil, err
	}
	conn.getLogger().Debug("executing format query", slog.String("sql", query), slog.String("format", format))
	return conn.queryFormat(ctx, ch.release, format, query, args...)
}

// InsertFormat executes the INSERT statement query, streaming data
// (pre-encoded in the given format) as the insert payload. See driver.Conn
// for the full contract.
//
// Experimental: this API is experimental and may change or be removed in a
// future minor release. It is currently only supported over the HTTP
// protocol; over the native protocol it returns ErrFormatNativeUnsupported.
func (ch *clickhouse) InsertFormat(ctx context.Context, format string, query string, data io.Reader) error {
	if err := validateFormatName(format); err != nil {
		return err
	}
	conn, err := ch.acquire(ctx)
	if err != nil {
		return err
	}
	conn.getLogger().Debug("executing format insert", slog.String("sql", query), slog.String("format", format))
	return conn.insertFormat(ctx, ch.release, format, query, data)
}
