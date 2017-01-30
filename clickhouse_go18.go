// +build go1.8

package clickhouse

import (
	"context"
	"database/sql/driver"
)

func (ch *clickhouse) Ping(ctx context.Context) error {
	return ch.ping()
}

func (ch *clickhouse) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return ch.beginTx(ctx, txOptions{
		Isolation: int(opts.Isolation),
		ReadOnly:  opts.ReadOnly,
	})
}

func (ch *clickhouse) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return ch.prepareContext(ctx, query)
}

func (stmt *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	dargs := make([]namedValue, len(args))
	for i, nv := range args {
		dargs[i] = namedValue(nv)
	}
	return stmt.queryContext(ctx, dargs)
}

func (stmt *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	dargs := make([]driver.Value, len(args))
	for i, nv := range args {
		dargs[i] = nv.Value
	}
	return stmt.execContext(ctx, dargs)
}
