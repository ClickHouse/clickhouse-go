package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"reflect"
	"sync/atomic"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var globalConnID int64

type stdConnOpener struct {
	err    error
	opt    *Options
	debugf func(format string, v ...any)
}

func (o *stdConnOpener) Driver() driver.Driver {
	var debugf = func(format string, v ...any) {}
	if o.opt.Debug {
		if o.opt.Debugf != nil {
			debugf = o.opt.Debugf
		} else {
			debugf = log.New(os.Stdout, "[clickhouse-std] ", 0).Printf
		}
	}
	return &stdDriver{
		opt:    o.opt,
		debugf: debugf,
	}
}

func (o *stdConnOpener) Connect(ctx context.Context) (_ driver.Conn, err error) {
	if o.err != nil {
		o.debugf("[connect] opener error: %v\n", o.err)
		return nil, o.err
	}
	var (
		conn     stdConnect
		connID   = int(atomic.AddInt64(&globalConnID, 1))
		dialFunc func(ctx context.Context, addr string, num int, opt *Options) (stdConnect, error)
	)

	switch o.opt.Protocol {
	case HTTP:
		dialFunc = func(ctx context.Context, addr string, num int, opt *Options) (stdConnect, error) {
			return dialHttp(ctx, addr, num, opt)
		}
	default:
		dialFunc = func(ctx context.Context, addr string, num int, opt *Options) (stdConnect, error) {
			return dial(ctx, addr, num, opt)
		}
	}

	if o.opt.Addr == nil || len(o.opt.Addr) == 0 {
		return nil, ErrAcquireConnNoAddress
	}

	for i := range o.opt.Addr {
		var num int
		switch o.opt.ConnOpenStrategy {
		case ConnOpenInOrder:
			num = i
		case ConnOpenRoundRobin:
			num = (connID + i) % len(o.opt.Addr)
		case ConnOpenRandom:
			random := rand.Int()
			num = (random + i) % len(o.opt.Addr)
		}
		if conn, err = dialFunc(ctx, o.opt.Addr[num], connID, o.opt); err == nil {
			var debugf = func(format string, v ...any) {}
			if o.opt.Debug {
				if o.opt.Debugf != nil {
					debugf = o.opt.Debugf
				} else {
					debugf = log.New(os.Stdout, fmt.Sprintf("[clickhouse-std][conn=%d][%s] ", num, o.opt.Addr[num]), 0).Printf
				}
			}
			return &stdDriver{
				conn:   conn,
				debugf: debugf,
			}, nil
		} else {
			o.debugf("[connect] error connecting to %s on connection %d: %v\n", o.opt.Addr[num], connID, err)
		}
	}

	return nil, err
}

var _ driver.Connector = (*stdConnOpener)(nil)

func init() {
	var debugf = func(format string, v ...any) {}
	sql.Register("clickhouse", &stdDriver{debugf: debugf})
}

// isConnBrokenError returns true if the error class indicates that the
// db connection is no longer usable and should be marked bad
func isConnBrokenError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	if _, ok := err.(*net.OpError); ok {
		return true
	}
	return false
}

func Connector(opt *Options) driver.Connector {
	if opt == nil {
		opt = &Options{}
	}

	o := opt.setDefaults()

	var debugf = func(format string, v ...any) {}
	if o.Debug {
		if o.Debugf != nil {
			debugf = o.Debugf
		} else {
			debugf = log.New(os.Stdout, "[clickhouse-std][opener] ", 0).Printf
		}
	}
	return &stdConnOpener{
		opt:    o,
		debugf: debugf,
	}
}

func OpenDB(opt *Options) *sql.DB {
	var debugf = func(format string, v ...any) {}
	if opt == nil {
		opt = &Options{}
	}

	o := opt.setDefaults()
	db := sql.OpenDB(&stdConnOpener{
		opt:    o,
		debugf: debugf,
	})

	// Ok to set these configs irrespective of values in opt.
	// Because opt.setDefaults() would have set some sane values
	// for these configs.
	db.SetMaxIdleConns(o.MaxIdleConns)
	db.SetMaxOpenConns(o.MaxOpenConns)
	db.SetConnMaxLifetime(o.ConnMaxLifetime)
	if o.Debug {
		if o.Debugf != nil {
			debugf = o.Debugf
		} else {
			debugf = log.New(os.Stdout, "[clickhouse-std][opener] ", 0).Printf
		}
	}

	return db
}

type stdConnect interface {
	isBad() bool
	close() error
	query(ctx context.Context, release nativeTransportRelease, query string, args ...any) (*rows, error)
	exec(ctx context.Context, query string, args ...any) error
	ping(ctx context.Context) (err error)
	prepareBatch(ctx context.Context, release nativeTransportRelease, acquire nativeTransportAcquire, query string, options chdriver.PrepareBatchOptions) (chdriver.Batch, error)
	asyncInsert(ctx context.Context, query string, wait bool, args ...any) error
}

type stdDriver struct {
	opt    *Options
	conn   stdConnect
	commit func() error
	debugf func(format string, v ...any)
}

// stdSession represents a stateful connection for the standard SQL driver
type stdSession struct {
	conn   stdConnect
	debugf func(format string, v ...any)
	closed bool
}

// stdSessionRow represents a single row result for session queries
type stdSessionRow struct {
	err  error
	rows *rows
}

func (r *stdSessionRow) Err() error {
	return r.err
}

func (r *stdSessionRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if !r.rows.Next() {
		r.rows.Close()
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := r.rows.Scan(dest...); err != nil {
		return err
	}
	return r.rows.Close()
}

func (r *stdSessionRow) ScanStruct(dest any) error {
	if r.err != nil {
		return r.err
	}
	values, err := r.rows.structMap.Map("ScanStruct", r.rows.columns, dest, true)
	if err != nil {
		return err
	}
	return r.Scan(values...)
}

func (s *stdSession) Exec(ctx context.Context, query string, args ...any) error {
	if s.closed {
		return ErrSessionClosed
	}
	s.debugf("[std session exec] \"%s\"", query)
	return s.conn.exec(ctx, query, args...)
}

func (s *stdSession) Query(ctx context.Context, query string, args ...any) (chdriver.Rows, error) {
	if s.closed {
		return nil, ErrSessionClosed
	}
	s.debugf("[std session query] \"%s\"", query)
	r, err := s.conn.query(ctx, func(nativeTransport, error) {}, query, args...)
	if err != nil {
		return nil, err
	}
	return &sessionRows{
		stdRows: &stdRows{
			rows:   r,
			debugf: s.debugf,
		},
	}, nil
}

func (s *stdSession) QueryRow(ctx context.Context, query string, args ...any) chdriver.Row {
	if s.closed {
		return &stdSessionRow{err: ErrSessionClosed}
	}
	s.debugf("[std session query row] \"%s\"", query)
	r, err := s.conn.query(ctx, func(nativeTransport, error) {}, query, args...)
	if err != nil {
		return &stdSessionRow{err: err}
	}
	return &stdSessionRow{rows: r}
}

func (s *stdSession) PrepareBatch(ctx context.Context, query string, opts ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	if s.closed {
		return nil, ErrSessionClosed
	}
	s.debugf("[std session prepare batch] \"%s\"", query)

	// PrepareBatch is not supported in std driver sessions
	return nil, ErrSessionNotSupported
}

func (s *stdSession) Ping(ctx context.Context) error {
	if s.closed {
		return ErrSessionClosed
	}
	s.debugf("[std session ping]")
	return s.conn.ping(ctx)
}

func (s *stdSession) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.debugf("[std session close]")
	return s.conn.close()
}

var _ driver.Conn = (*stdDriver)(nil)
var _ driver.ConnBeginTx = (*stdDriver)(nil)
var _ driver.ExecerContext = (*stdDriver)(nil)
var _ driver.QueryerContext = (*stdDriver)(nil)
var _ driver.ConnPrepareContext = (*stdDriver)(nil)

func (std *stdDriver) Open(dsn string) (_ driver.Conn, err error) {
	var opt Options
	if err := opt.fromDSN(dsn); err != nil {
		std.debugf("Open dsn error: %v\n", err)
		return nil, err
	}
	o := opt.setDefaults()
	var debugf = func(format string, v ...any) {}
	if o.Debug {
		debugf = log.New(os.Stdout, "[clickhouse-std][opener] ", 0).Printf
	}
	o.ClientInfo.comment = []string{"database/sql"}
	return (&stdConnOpener{opt: o, debugf: debugf}).Connect(context.Background())
}

func (std *stdDriver) AcquireSession(ctx context.Context) (chdriver.Session, error) {
	if std.conn.isBad() {
		std.debugf("AcquireSession: connection is bad")
		return nil, driver.ErrBadConn
	}

	return &stdSession{
		conn:   std.conn,
		debugf: std.debugf,
	}, nil
}

var _ driver.Driver = (*stdDriver)(nil)

func (std *stdDriver) ResetSession(ctx context.Context) error {
	if std.conn.isBad() {
		std.debugf("Resetting session because connection is bad")
		return driver.ErrBadConn
	}
	return nil
}

var _ driver.SessionResetter = (*stdDriver)(nil)

func (std *stdDriver) Ping(ctx context.Context) error {
	if std.conn.isBad() {
		std.debugf("Ping: connection is bad")
		return driver.ErrBadConn
	}

	return std.conn.ping(ctx)
}

var _ driver.Pinger = (*stdDriver)(nil)

func (std *stdDriver) Begin() (driver.Tx, error) {
	if std.conn.isBad() {
		std.debugf("Begin: connection is bad")
		return nil, driver.ErrBadConn
	}

	return std, nil
}

func (std *stdDriver) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if std.conn.isBad() {
		std.debugf("BeginTx: connection is bad")
		return nil, driver.ErrBadConn
	}

	return std, nil
}

func (std *stdDriver) Commit() error {
	if std.commit == nil {
		return nil
	}
	defer func() {
		std.commit = nil
	}()

	if err := std.commit(); err != nil {
		if isConnBrokenError(err) {
			std.debugf("Commit got EOF error: resetting connection")
			return driver.ErrBadConn
		}
		std.debugf("Commit error: %v\n", err)
		return err
	}
	return nil
}

func (std *stdDriver) Rollback() error {
	std.commit = nil
	std.conn.close()
	return nil
}

var _ driver.Tx = (*stdDriver)(nil)

func (std *stdDriver) CheckNamedValue(nv *driver.NamedValue) error { return nil }

var _ driver.NamedValueChecker = (*stdDriver)(nil)

func (std *stdDriver) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if std.conn.isBad() {
		std.debugf("ExecContext: connection is bad")
		return nil, driver.ErrBadConn
	}

	var err error
	if asyncOpt := queryOptionsAsync(ctx); asyncOpt.ok {
		err = std.conn.asyncInsert(ctx, query, asyncOpt.wait, rebind(args)...)
	} else {
		err = std.conn.exec(ctx, query, rebind(args)...)
	}

	if err != nil {
		if isConnBrokenError(err) {
			std.debugf("ExecContext got a fatal error, resetting connection: %v\n", err)
			return nil, driver.ErrBadConn
		}
		std.debugf("ExecContext error: %v\n", err)
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (std *stdDriver) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if std.conn.isBad() {
		std.debugf("QueryContext: connection is bad")
		return nil, driver.ErrBadConn
	}

	r, err := std.conn.query(ctx, func(nativeTransport, error) {}, query, rebind(args)...)
	if isConnBrokenError(err) {
		std.debugf("QueryContext got a fatal error, resetting connection: %v\n", err)
		return nil, driver.ErrBadConn
	}
	if err != nil {
		std.debugf("QueryContext error: %v\n", err)
		return nil, err
	}
	return &stdRows{
		rows:   r,
		debugf: std.debugf,
	}, nil
}

func (std *stdDriver) Prepare(query string) (driver.Stmt, error) {
	return std.PrepareContext(context.Background(), query)
}

func (std *stdDriver) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if std.conn.isBad() {
		std.debugf("PrepareContext: connection is bad")
		return nil, driver.ErrBadConn
	}

	batch, err := std.conn.prepareBatch(ctx, func(nativeTransport, error) {}, func(context.Context) (nativeTransport, error) { return nil, nil }, query, chdriver.PrepareBatchOptions{})
	if err != nil {
		if isConnBrokenError(err) {
			std.debugf("PrepareContext got a fatal error, resetting connection: %v\n", err)
			return nil, driver.ErrBadConn
		}
		std.debugf("PrepareContext error: %v\n", err)
		return nil, err
	}
	std.commit = batch.Send
	return &stdBatch{
		batch:  batch,
		debugf: std.debugf,
	}, nil
}

func (std *stdDriver) Close() error {
	err := std.conn.close()
	if err != nil {
		if isConnBrokenError(err) {
			std.debugf("Close got a fatal error, resetting connection: %v\n", err)
			return driver.ErrBadConn
		}
		std.debugf("Close error: %v\n", err)
	}
	return err
}

type stdBatch struct {
	batch  chdriver.Batch
	debugf func(format string, v ...any)
}

func (s *stdBatch) NumInput() int { return -1 }
func (s *stdBatch) Exec(args []driver.Value) (driver.Result, error) {
	values := make([]any, 0, len(args))
	for _, v := range args {
		values = append(values, v)
	}
	if err := s.batch.Append(values...); err != nil {
		s.debugf("[batch][exec] append error: %v", err)
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (s *stdBatch) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	values := make([]driver.Value, 0, len(args))
	for _, v := range args {
		values = append(values, v.Value)
	}
	return s.Exec(values)
}

var _ driver.StmtExecContext = (*stdBatch)(nil)

func (s *stdBatch) Query(args []driver.Value) (driver.Rows, error) {
	// Note: not implementing driver.StmtQueryContext accordingly
	return nil, errors.New("only Exec method supported in batch mode")
}

func (s *stdBatch) Close() error { return nil }

type stdRows struct {
	rows   *rows
	debugf func(format string, v ...any)
}

func (r *stdRows) Columns() []string {
	return r.rows.Columns()
}

func (r *stdRows) ColumnTypeScanType(idx int) reflect.Type {
	return r.rows.block.Columns[idx].ScanType()
}

var _ driver.RowsColumnTypeScanType = (*stdRows)(nil)

func (r *stdRows) ColumnTypeDatabaseTypeName(idx int) string {
	return string(r.rows.block.Columns[idx].Type())
}

func (r *stdRows) ColumnTypeNullable(idx int) (nullable, ok bool) {
	_, ok = r.rows.block.Columns[idx].(*column.Nullable)
	return ok, true
}

func (r *stdRows) ColumnTypePrecisionScale(idx int) (precision, scale int64, ok bool) {
	switch col := r.rows.block.Columns[idx].(type) {
	case *column.Decimal:
		return col.Precision(), col.Scale(), true
	case *column.DateTime64:
		p, ok := col.Precision()
		return p, 0, ok
	case interface{ Base() column.Interface }:
		switch col := col.Base().(type) {
		case *column.Decimal:
			return col.Precision(), col.Scale(), true
		case *column.DateTime64:
			p, ok := col.Precision()
			return p, 0, ok
		}
	}
	return 0, 0, false
}

var _ driver.Rows = (*stdRows)(nil)
var _ driver.RowsNextResultSet = (*stdRows)(nil)
var _ driver.RowsColumnTypeDatabaseTypeName = (*stdRows)(nil)
var _ driver.RowsColumnTypeNullable = (*stdRows)(nil)
var _ driver.RowsColumnTypePrecisionScale = (*stdRows)(nil)

func (r *stdRows) Next(dest []driver.Value) error {
	if len(r.rows.block.Columns) != len(dest) {
		err := fmt.Errorf("expected %d destination arguments in Next, not %d", len(r.rows.block.Columns), len(dest))
		r.debugf("Next length error: %v\n", err)
		return &OpError{
			Op:  "Next",
			Err: err,
		}
	}
	if r.rows.Next() {
		for i := range dest {
			nullable, ok := r.ColumnTypeNullable(i)
			switch value := r.rows.block.Columns[i].Row(r.rows.row-1, nullable && ok).(type) {
			case driver.Valuer:
				v, err := value.Value()
				if err != nil {
					r.debugf("Next row error: %v\n", err)
					return err
				}
				dest[i] = v
			default:
				// We don't know what is the destination type at this stage,
				// but destination type might be a sql.Null* type that expects to receive a value
				// instead of a pointer to a value. ClickHouse-go returns pointers to values for nullable columns.
				//
				// This is a compatibility layer to make sure that the driver works with the standard library.
				// Due to reflection used it has a performance cost.
				if nullable {
					if value == nil {
						dest[i] = nil
						continue
					}
					rv := reflect.ValueOf(value)
					value = rv.Elem().Interface()
				}

				dest[i] = value
			}
		}
		return nil
	}
	if err := r.rows.Err(); err != nil {
		r.debugf("Next rows error: %v\n", err)
		return err
	}
	return io.EOF
}

func (r *stdRows) HasNextResultSet() bool {
	return r.rows.totals != nil
}

func (r *stdRows) NextResultSet() error {
	switch {
	case r.rows.totals != nil:
		r.rows.block = r.rows.totals
		r.rows.totals = nil
	default:
		return io.EOF
	}
	return nil
}

var _ driver.RowsNextResultSet = (*stdRows)(nil)

func (r *stdRows) Close() error {
	err := r.rows.Close()
	if err != nil {
		r.debugf("Rows Close error: %v\n", err)
	}
	return err
}

var _ driver.Rows = (*stdRows)(nil)

// sessionRows wraps stdRows to implement chdriver.Rows interface
type sessionRows struct {
	*stdRows
}

func (r *sessionRows) Next() bool {
	return r.stdRows.Next([]driver.Value{}) == nil
}

func (r *sessionRows) Scan(dest ...any) error {
	// Convert dest to driver.Value slice
	values := make([]driver.Value, len(dest))
	for i, d := range dest {
		values[i] = d
	}
	return r.stdRows.Next(values)
}

func (r *sessionRows) ScanStruct(dest any) error {
	// ScanStruct is not implemented for session rows in std driver
	return errors.New("ScanStruct not implemented for session rows")
}

func (r *sessionRows) ColumnTypes() []chdriver.ColumnType {
	// ColumnTypes is not implemented for session rows in std driver
	return nil
}

func (r *sessionRows) Totals(dest ...any) error {
	// Totals is not implemented for session rows in std driver
	return errors.New("Totals not implemented for session rows")
}

func (r *sessionRows) Columns() []string {
	return r.stdRows.Columns()
}

func (r *sessionRows) Close() error {
	return r.stdRows.Close()
}

func (r *sessionRows) Err() error {
	// This would need to be implemented based on the error handling
	return nil
}
