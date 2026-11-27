package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
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
	logger *slog.Logger
}

func (o *stdConnOpener) Driver() driver.Driver {
	return &stdDriver{
		opt:    o.opt,
		logger: o.logger,
	}
}

func (o *stdConnOpener) Connect(ctx context.Context) (_ driver.Conn, err error) {
	if o.err != nil {
		o.logger.Error("opener error", slog.Any("error", o.err))
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

	if len(o.opt.Addr) == 0 {
		return nil, ErrAcquireConnNoAddress
	}

	var offset int
	if o.opt.ConnOpenStrategy == ConnOpenRandom {
		offset = rand.IntN(len(o.opt.Addr))
	}

	for i := range o.opt.Addr {
		var num int
		switch o.opt.ConnOpenStrategy {
		case ConnOpenInOrder:
			num = i
		case ConnOpenRoundRobin:
			num = (connID + i) % len(o.opt.Addr)
		case ConnOpenRandom:
			num = (offset + i) % len(o.opt.Addr)
		}
		if conn, err = dialFunc(ctx, o.opt.Addr[num], connID, o.opt); err == nil {
			// Create a logger with connection-specific context
			connLogger := o.logger.With(
				slog.Int("conn_num", num),
				slog.String("addr", o.opt.Addr[num]),
			)
			return &stdDriver{
				conn:   conn,
				logger: connLogger,
			}, nil
		} else {
			o.logger.Error("connection error",
				slog.String("addr", o.opt.Addr[num]),
				slog.Int("conn_id", connID),
				slog.Any("error", err))
		}
	}

	return nil, err
}

var _ driver.Connector = (*stdConnOpener)(nil)

func init() {
	sql.Register("clickhouse", &stdDriver{logger: newNoopLogger()})
}

// isConnBrokenError returns true if the error class indicates that the
// db connection is no longer usable and should be marked bad
func isConnBrokenError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

func Connector(opt *Options) driver.Connector {
	if opt == nil {
		opt = &Options{}
	}

	validationErr := opt.validate()
	o := opt.setDefaults()
	logger := o.logger().With(slog.String("component", "std-driver"))

	return &stdConnOpener{
		err:    validationErr,
		opt:    o,
		logger: logger,
	}
}

func OpenDB(opt *Options) *sql.DB {
	if opt == nil {
		opt = &Options{}
	}

	validationErr := opt.validate()
	o := opt.setDefaults()
	logger := o.logger().With(slog.String("component", "std-driver"))

	db := sql.OpenDB(&stdConnOpener{
		err:    validationErr,
		opt:    o,
		logger: logger,
	})

	// Ok to set these configs irrespective of values in opt.
	// Because opt.setDefaults() would have set some sane values
	// for these configs.
	db.SetMaxIdleConns(o.MaxIdleConns)
	db.SetMaxOpenConns(o.MaxOpenConns)
	db.SetConnMaxLifetime(o.ConnMaxLifetime)

	return db
}

type stdConnect interface {
	// healthCheck reports why the connection is unusable; nil means healthy.
	healthCheck() error
	close() error
	resetWriteState()
	hasWrittenBytes() bool
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
	logger *slog.Logger
	broken atomic.Bool
}

var _ driver.Conn = (*stdDriver)(nil)
var _ driver.ConnBeginTx = (*stdDriver)(nil)
var _ driver.ExecerContext = (*stdDriver)(nil)
var _ driver.QueryerContext = (*stdDriver)(nil)
var _ driver.ConnPrepareContext = (*stdDriver)(nil)

func (std *stdDriver) Open(dsn string) (_ driver.Conn, err error) {
	var opt Options
	if err := opt.fromDSN(dsn); err != nil {
		std.logger.Error("dsn parsing error", slog.Any("error", err))
		return nil, err
	}
	if err := opt.validate(); err != nil {
		return nil, err
	}
	o := opt.setDefaults()
	logger := o.logger().With(slog.String("component", "std-driver"))
	o.ClientInfo.Comment = []string{"database/sql"}
	return (&stdConnOpener{opt: o, logger: logger}).Connect(context.Background())
}

var _ driver.Driver = (*stdDriver)(nil)

func (std *stdDriver) markBroken() {
	std.broken.Store(true)
}

// IsValid implements driver.Validator so database/sql discards broken
// connections on return without duplicating the healthCheck done in ResetSession.
func (std *stdDriver) IsValid() bool {
	return std.conn != nil && !std.broken.Load()
}

var _ driver.Validator = (*stdDriver)(nil)

func (std *stdDriver) checkHealth(op string) error {
	if std.broken.Load() {
		std.logger.Debug(op + ": connection is marked broken")
		return driver.ErrBadConn
	}
	if err := std.conn.healthCheck(); err != nil {
		std.markBroken()
		std.logger.Debug(op+": connection is bad", slog.Any("reason", err))
		return driver.ErrBadConn
	}
	return nil
}

func (std *stdDriver) handleOpError(ctx context.Context, op string, err error) error {
	if errors.Is(err, driver.ErrBadConn) {
		std.markBroken()
		if !std.conn.hasWrittenBytes() && (ctx == nil || ctx.Err() == nil) {
			return driver.ErrBadConn
		}
		sanitized := fmt.Errorf("%s failed after send: %v", op, err)
		std.logger.Error(op+" error after sending", slog.Any("error", sanitized))
		return sanitized
	}
	if ctx != nil && ctx.Err() != nil {
		if std.conn.hasWrittenBytes() || std.conn.healthCheck() != nil {
			std.markBroken()
		}
		std.logger.Error(op+" error", slog.Any("error", err))
		return err
	}
	if isConnBrokenError(err) {
		std.markBroken()
		if !std.conn.hasWrittenBytes() {
			std.logger.Error(op+" got a fatal error before sending, resetting connection", slog.Any("error", err))
			return driver.ErrBadConn
		}
		std.logger.Error(op+" got a fatal error after sending, not retrying", slog.Any("error", err))
		return err
	}
	if std.conn.healthCheck() != nil {
		std.markBroken()
	}
	std.logger.Error(op+" error", slog.Any("error", err))
	return err
}

func (std *stdDriver) ResetSession(ctx context.Context) error {
	return std.checkHealth("resetting session")
}

var _ driver.SessionResetter = (*stdDriver)(nil)

func (std *stdDriver) Ping(ctx context.Context) error {
	if err := std.checkHealth("ping"); err != nil {
		return err
	}

	if err := std.conn.ping(ctx); err != nil {
		if isConnBrokenError(err) || (ctx != nil && ctx.Err() != nil) || std.conn.healthCheck() != nil {
			std.markBroken()
		}
		return err
	}
	return nil
}

var _ driver.Pinger = (*stdDriver)(nil)

func (std *stdDriver) Begin() (driver.Tx, error) {
	if err := std.checkHealth("begin"); err != nil {
		return nil, err
	}

	return std, nil
}

func (std *stdDriver) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := std.checkHealth("begin tx"); err != nil {
		return nil, err
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
		if isConnBrokenError(err) || std.conn.healthCheck() != nil {
			std.markBroken()
		}
		if isConnBrokenError(err) {
			std.logger.Debug("commit got EOF error: resetting connection")
			return driver.ErrBadConn
		}
		std.logger.Error("commit error", slog.Any("error", err))
		return err
	}
	return nil
}

func (std *stdDriver) Rollback() error {
	std.commit = nil
	std.markBroken()
	std.conn.close()
	return nil
}

var _ driver.Tx = (*stdDriver)(nil)

func (std *stdDriver) CheckNamedValue(nv *driver.NamedValue) error { return nil }

var _ driver.NamedValueChecker = (*stdDriver)(nil)

func (std *stdDriver) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := std.checkHealth("exec context"); err != nil {
		return nil, err
	}

	std.conn.resetWriteState()

	var err error
	if asyncOpt := queryOptionsAsync(ctx); asyncOpt.ok {
		err = std.conn.asyncInsert(ctx, query, asyncOpt.wait, rebind(args)...)
	} else {
		err = std.conn.exec(ctx, query, rebind(args)...)
	}

	if err != nil {
		return nil, std.handleOpError(ctx, "exec context", err)
	}
	return driver.RowsAffected(0), nil
}

func (std *stdDriver) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := std.checkHealth("query context"); err != nil {
		return nil, err
	}

	std.conn.resetWriteState()

	r, err := std.conn.query(ctx, func(nativeTransport, error) {}, query, rebind(args)...)
	if err != nil {
		return nil, std.handleOpError(ctx, "query context", err)
	}
	return &stdRows{
		rows:        r,
		logger:      std.logger,
		onStreamErr: std.markBrokenIfFatal,
	}, nil
}

func (std *stdDriver) Prepare(query string) (driver.Stmt, error) {
	return std.PrepareContext(context.Background(), query)
}

func (std *stdDriver) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := std.checkHealth("prepare context"); err != nil {
		return nil, err
	}

	std.conn.resetWriteState()

	batch, err := std.conn.prepareBatch(ctx, func(nativeTransport, error) {}, func(context.Context) (nativeTransport, error) { return nil, nil }, query, chdriver.PrepareBatchOptions{})
	if err != nil {
		return nil, std.handleOpError(ctx, "prepare context", err)
	}
	std.commit = batch.Send
	return &stdBatch{
		batch:  batch,
		logger: std.logger,
	}, nil
}

func (std *stdDriver) Close() error {
	std.markBroken()
	err := std.conn.close()
	if err != nil {
		if isConnBrokenError(err) {
			std.logger.Error("close got a fatal error, resetting connection", slog.Any("error", err))
			return driver.ErrBadConn
		}
		std.logger.Error("close error", slog.Any("error", err))
	}
	return err
}

func (std *stdDriver) markBrokenIfFatal(err error) {
	if isConnBrokenError(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (std.conn != nil && std.conn.healthCheck() != nil) {
		std.markBroken()
	}
}

type stdBatch struct {
	batch  chdriver.Batch
	logger *slog.Logger
}

func (s *stdBatch) NumInput() int { return -1 }
func (s *stdBatch) Exec(args []driver.Value) (driver.Result, error) {
	values := make([]any, 0, len(args))
	for _, v := range args {
		values = append(values, v)
	}
	if err := s.batch.Append(values...); err != nil {
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
	rows        *rows
	logger      *slog.Logger
	onStreamErr func(error)
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
		r.logger.Error("next length error", slog.Any("error", err))
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
					r.logger.Error("next row error", slog.Any("error", err))
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
		if r.onStreamErr != nil {
			r.onStreamErr(err)
		}
		r.logger.Error("next rows error", slog.Any("error", err))
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
		if r.onStreamErr != nil {
			r.onStreamErr(err)
		}
		r.logger.Error("rows close error", slog.Any("error", err))
	}
	return err
}

var _ driver.Rows = (*stdRows)(nil)
