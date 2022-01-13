package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"regexp"
	"strings"
	"sync/atomic"
)

func init() {
	sql.Register("clickhouse", &stdDriver{})
}

type stdDriver struct {
	conn   *connect
	commit func() error
	connID int64
}

func (d *stdDriver) Open(dsn string) (_ driver.Conn, err error) {
	var (
		opt    Options
		conn   *connect
		connID = int(atomic.AddInt64(&d.connID, 1))
	)
	if err = opt.fromDSN(dsn); err != nil {
		return nil, err
	}
	for num := range opt.Addr {
		if opt.ConnOpenStrategy == ConnOpenRoundRobin {
			num = int(connID) % len(opt.Addr)
		}
		if conn, err = dial(opt.Addr[num], connID, &opt); err == nil {
			return &stdDriver{
				conn: conn,
			}, nil
		}
	}
	return nil, err
}

func (std *stdDriver) Ping(ctx context.Context) error { return std.conn.ping(ctx) }

func (std *stdDriver) Begin() (driver.Tx, error) { return std, nil }

func (std *stdDriver) Commit() error {
	if std.commit == nil {
		return nil
	}
	defer func() {
		std.commit = nil
	}()
	return std.commit()
}

func (std *stdDriver) Rollback() error {
	std.commit = nil
	std.conn.close()
	return nil
}

func (std *stdDriver) CheckNamedValue(nv *driver.NamedValue) error { return nil }

func (std *stdDriver) Close() error { return std.conn.close() }

func (std *stdDriver) Prepare(query string) (driver.Stmt, error) {
	return std.PrepareContext(context.Background(), query)
}

func (std *stdDriver) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if isInsert(query) {
		return std.insert(ctx, query)
	}
	return &stmt{
		conn:  std.conn,
		query: query,
	}, nil
}

func (std *stdDriver) insert(ctx context.Context, query string) (driver.Stmt, error) {
	batch, err := std.conn.prepareBatch(ctx, query, func(c *connect) {})
	if err != nil {
		return nil, err
	}
	std.commit = batch.Send
	return &stdBatch{
		batch: batch,
	}, nil
}

type stdBatch struct {
	batch *batch
}

func (s *stdBatch) NumInput() int { return -1 }
func (s *stdBatch) Exec(args []driver.Value) (driver.Result, error) {
	values := make([]interface{}, 0, len(args))
	for _, v := range args {
		values = append(values, v)
	}
	if err := s.batch.Append(values...); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (s *stdBatch) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("only Exec method supported in batch mode")
}

func (s *stdBatch) Close() error { return nil }

type stmt struct {
	conn  *connect
	query string
}

func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), driverValueToNamedValue(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.conn.exec(ctx, s.query, rebind(args)...); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), driverValueToNamedValue(args))
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	r, err := s.conn.query(ctx, s.query, rebind(args)...)
	if err != nil {
		return nil, err
	}
	return &stdRows{
		rows: r,
	}, nil
}

func (s *stmt) Close() error {

	return nil
}

type stdRows struct {
	rows *rows
}

func (r *stdRows) Columns() []string {
	return r.rows.Columns()
}

func (r *stdRows) Next(dest []driver.Value) error {
	if r.rows.Next() {
		for i := range dest {
			dest[i] = r.rows.block.Columns[i].Row(r.rows.row - 1)
		}
		return nil
	}
	return io.EOF
}

func (r *stdRows) Close() error {
	return r.rows.Close()
}

var selectRe = regexp.MustCompile(`\s+SELECT\s+`)

func isInsert(query string) bool {
	if f := strings.Fields(query); len(f) > 2 {
		return strings.EqualFold("INSERT", f[0]) &&
			strings.EqualFold("INTO", f[1]) &&
			!selectRe.MatchString(strings.ToUpper(query))
	}
	return false
}

func driverValueToNamedValue(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, 0, len(args))
	for i, v := range args {
		named = append(named, driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		})
	}
	return named
}
