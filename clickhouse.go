package clickhouse

import (
	"context"
	"crypto/tls"
	"io"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/compress"
	"github.com/ClickHouse/clickhouse-go/lib/driver"
	"github.com/ClickHouse/clickhouse-go/lib/proto"
)

func Named(name string, value interface{}) driver.NamedValue {
	return driver.NamedValue{
		Name:  name,
		Value: value,
	}
}

type (
	Date     time.Time
	DateTime time.Time
)

type (
	Progress      = proto.Progress
	Exception     = proto.Exception
	ServerVersion = proto.ServerHandshake
)

var (
	CompressionLZ4 compress.Method = compress.LZ4
)

type Auth struct { // has_control_character
	Database string
	Username string
	Password string
}

type Compression struct {
	Method compress.Method
}

type Options struct {
	Addr            []string
	Auth            Auth
	TLS             *tls.Config
	Debug           bool
	Settings        Settings
	DialTimeout     time.Duration
	Compression     *Compression
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

func (o *Options) setDefaults() {
	if o.DialTimeout == 0 {
		o.DialTimeout = time.Second
	}
	if o.MaxIdleConns <= 0 {
		o.MaxIdleConns = 5
	}
	if o.MaxOpenConns <= 0 {
		o.MaxOpenConns = o.MaxIdleConns + 5
	}
	if o.ConnMaxLifetime == 0 {
		o.ConnMaxLifetime = time.Hour
	}
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
	opt  *Options
	idle chan *connect
	open chan struct{}
}

func (ch *clickhouse) ServerVersion() (*driver.ServerVersion, error) {
	conn, err := ch.acquire()
	if err != nil {
		return nil, err
	}
	defer ch.release(conn)
	return &conn.server, nil
}

func (ch *clickhouse) Query(ctx context.Context, query string, args ...interface{}) (rows driver.Rows, err error) {
	conn, err := ch.acquire()
	if err != nil {
		return nil, err
	}
	defer ch.release(conn)
	return conn.query(ctx, query, args...)
}

func (ch *clickhouse) QueryBlock(ctx context.Context, query string, cb func(driver.Block), args ...interface{}) error {
	conn, err := ch.acquire()
	if err != nil {
		return err
	}
	defer ch.release(conn)
	return conn.queryBlock(ctx, query, cb, args...)
}

func (ch *clickhouse) Exec(ctx context.Context, query string, args ...interface{}) error {
	conn, err := ch.acquire()
	if err != nil {
		return err
	}
	defer ch.release(conn)
	return conn.exec(ctx, query, args...)
}

func (ch *clickhouse) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	conn, err := ch.acquire()
	if err != nil {
		return nil, err
	}
	return conn.prepareBatch(ctx, query, ch.release)
}

func (ch *clickhouse) Ping(ctx context.Context) error {
	conn, err := ch.acquire()
	if err != nil {
		return err
	}
	defer ch.release(conn)
	return conn.ping(ctx)
}

func (ch *clickhouse) Stats() driver.Stats {
	return driver.Stats{
		Open:         len(ch.open),
		Idle:         len(ch.idle),
		MaxOpenConns: cap(ch.open),
		MaxIdleConns: cap(ch.idle),
	}
}

func (ch *clickhouse) acquire() (conn *connect, err error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil, io.EOF
	case ch.open <- struct{}{}:
	}
	select {
	case <-timer.C:
		return nil, io.EOF
	case conn := <-ch.idle:
		return conn, nil
	default:
	}
	for _, addr := range ch.opt.Addr {
		if conn, err = dial(addr, ch.opt); err == nil {
			return
		}
	}
	return
}

func (ch *clickhouse) release(conn *connect) {
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
