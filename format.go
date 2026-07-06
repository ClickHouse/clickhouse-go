package clickhouse

import (
	"context"
	"io"
	"log/slog"
)

// QueryFormat executes query and returns the result encoded in the
// given ClickHouse format as a raw byte stream. See driver.Conn for the full
// contract.
func (ch *clickhouse) QueryFormat(ctx context.Context, format string, query string, args ...any) (io.ReadCloser, error) {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return nil, err
	}
	conn.getLogger().Debug("executing format query", slog.String("sql", query), slog.String("format", format))
	return conn.queryFormat(ctx, ch.release, format, query, args...)
}

// InsertFormat executes the INSERT statement query, streaming data
// (pre-encoded in the given format) as the insert payload. See driver.Conn for
// the full contract.
func (ch *clickhouse) InsertFormat(ctx context.Context, format string, query string, data io.Reader) error {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return err
	}
	conn.getLogger().Debug("executing format insert", slog.String("sql", query), slog.String("format", format))
	return conn.insertFormat(ctx, ch.release, format, query, data)
}
