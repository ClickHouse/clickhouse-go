package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type (
	Date     time.Time
	DateTime time.Time
)

type Conn = driver.Conn

type (
	Progress      = proto.Progress
	Exception     = proto.Exception
	ProfileInfo   = proto.ProfileInfo
	ServerVersion = proto.ServerHandshake
)

var (
	ErrBatchAlreadySent               = errors.New("clickhouse: batch has already been sent")
	ErrAcquireConnTimeout             = errors.New("clickhouse: acquire conn timeout")
	ErrUnsupportedServerRevision      = errors.New("clickhouse: unsupported server revision")
	ErrBindMixedNamedAndNumericParams = errors.New("clickhouse [bind]: mixed named and numeric parameters")
)

type OpError struct {
	Op         string
	ColumnName string
	Err        error
}

func (e *OpError) Error() string {
	switch err := e.Err.(type) {
	case *column.Error:
		return fmt.Sprintf("clickhouse [%s]: (%s %s) %s", e.Op, e.ColumnName, err.ColumnType, err.Err)
	case *column.ColumnConverterError:
		var hint string
		if len(err.Hint) != 0 {
			hint += ". " + err.Hint
		}
		return fmt.Sprintf("clickhouse [%s]: (%s) converting %s to %s is unsupported%s",
			err.Op, e.ColumnName,
			err.From, err.To,
			hint,
		)
	}
	return fmt.Sprintf("clickhouse [%s]: %s", e.Op, e.Err)
}

func Open(opt *Options) (driver.Conn, error) {
	opt.setDefaults()
	return &clickhouse{
		opt:  opt,
		idle: make(chan *connect, opt.MaxIdleConns),
		open: make(chan struct{}, opt.MaxOpenConns),
	}, nil
}

type clickhouse struct {
	opt    *Options
	idle   chan *connect
	open   chan struct{}
	connID int64
}

func (ch *clickhouse) ServerVersion() (*driver.ServerVersion, error) {
	var (
		ctx, cancel = context.WithTimeout(context.Background(), ch.opt.DialTimeout)
		conn, err   = ch.acquire(ctx)
	)
	defer cancel()
	if err != nil {
		return nil, err
	}
	defer ch.release(conn)
	return &conn.server, nil
}

func (ch *clickhouse) Query(ctx context.Context, query string, args ...interface{}) (rows driver.Rows, err error) {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer ch.release(conn)
	return conn.query(ctx, query, args...)
}

func (ch *clickhouse) QueryRow(ctx context.Context, query string, args ...interface{}) (rows driver.Row) {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return &row{
			err: err,
		}
	}
	defer ch.release(conn)
	return conn.queryRow(ctx, query, args...)
}

func (ch *clickhouse) Exec(ctx context.Context, query string, args ...interface{}) error {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return err
	}
	defer ch.release(conn)
	return conn.exec(ctx, query, args...)
}

func (ch *clickhouse) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return nil, err
	}
	return conn.prepareBatch(ctx, query, ch.release)
}

func (ch *clickhouse) Ping(ctx context.Context) error {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return err
	}
	defer ch.release(conn)
	return nil
}

func (ch *clickhouse) Stats() driver.Stats {
	return driver.Stats{
		Open:         len(ch.open),
		Idle:         len(ch.idle),
		MaxOpenConns: cap(ch.open),
		MaxIdleConns: cap(ch.idle),
	}
}

func (ch *clickhouse) dial() (conn *connect, err error) {
	connID := int(atomic.AddInt64(&ch.connID, 1))
	for num := range ch.opt.Addr {
		if ch.opt.ConnOpenStrategy == ConnOpenRoundRobin {
			num = int(connID) % len(ch.opt.Addr)
		}
		if conn, err = dial(ch.opt.Addr[num], connID, ch.opt); err == nil {
			return conn, nil
		}
	}
	return nil, err
}

func (ch *clickhouse) acquire(ctx context.Context) (conn *connect, err error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	select {
	case <-timer.C:
		return nil, ErrAcquireConnTimeout
	case ch.open <- struct{}{}:
	}
	select {
	case <-timer.C:
		return nil, ErrAcquireConnTimeout
	case conn := <-ch.idle:
		if conn.isBad() {
			conn.close()
			return ch.dial()
		}
		conn.released = false
		return conn, nil
	default:
	}
	return ch.dial()
}

func (ch *clickhouse) release(conn *connect) {
	if conn.released {
		return
	}
	conn.released = true
	select {
	case <-ch.open:
	default:
	}
	if conn.err != nil || time.Since(conn.connectedAt) >= ch.opt.ConnMaxLifetime {
		conn.close()
		return
	}
	select {
	case ch.idle <- conn:
	default:
		conn.close()
	}
}

func (ch *clickhouse) Close() error {
	for {
		select {
		case c := <-ch.idle:
			c.close()
		default:
			return nil
		}
	}
}
