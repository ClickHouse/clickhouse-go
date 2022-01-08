package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"
)

func init() {
	driver := STDDriver{}
	sql.Register("clickhouse", &driver)
}

type STDDriver struct {
	counter uint64
	conn    *connect
	commit  func() error
}

func (d *STDDriver) Open(dsn string) (driver.Conn, error) {
	conn, err := dial("127.0.0.1:9000", &Options{
		Auth: Auth{
			Database: "default",
			Username: "default",
		},
		DialTimeout: time.Second,
		Compression: &Compression{
			Method: CompressionLZ4,
		},
		Debug: true,
	})
	if err != nil {
		return nil, err
	}
	return &STDDriver{
		conn: conn,
	}, nil
}

func (d *STDDriver) Begin() (driver.Tx, error) {
	return d, nil
}

func (std *STDDriver) Ping(ctx context.Context) error {
	return std.conn.ping(ctx)
}

func (std *STDDriver) Commit() error {
	if std.commit == nil {
		return nil
	}
	defer func() {
		std.commit = nil
	}()
	return std.commit()
}

func (std *STDDriver) Rollback() error {
	std.commit = nil
	return nil
}

func (std *STDDriver) CheckNamedValue(nv *driver.NamedValue) error {
	return nil
}

func (std *STDDriver) Close() error {
	return std.conn.close()
}

func (std *STDDriver) Prepare(query string) (driver.Stmt, error) {
	return std.PrepareContext(context.Background(), query)
}

func (std *STDDriver) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if isInsert(query) {
		return std.insert(ctx, query)
	}
	return &stmt{
		conn:  std.conn,
		query: query,
	}, nil
}

func (std *STDDriver) insert(ctx context.Context, query string) (driver.Stmt, error) {
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

func (s *stdBatch) Close() error {
	return nil
}

type stmt struct {
	conn  *connect
	query string
}

func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	var params []driver.NamedValue
	for i, v := range args {
		params = append(params, driver.NamedValue{
			Ordinal: i,
			Value:   v,
		})
	}
	return s.ExecContext(context.Background(), params)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.conn.exec(ctx, s.query); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	var params []driver.NamedValue
	for i, v := range args {
		params = append(params, driver.NamedValue{
			Ordinal: i,
			Value:   v,
		})
	}
	return s.QueryContext(context.Background(), params)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	r, err := s.conn.query(ctx, s.query)
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
			dest[i] = r.rows.block.Columns[i].RowValue(r.rows.row - 1)
		}
		return nil
	}
	return io.EOF
}

func (r *stdRows) Close() error {
	return nil
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
