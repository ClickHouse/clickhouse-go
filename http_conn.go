package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

func init() {
	sql.Register("clickhousehttp", new(httpDriver))
}

var _ httpDriverI = (*httpConnOpener)(nil)

type httpDriverI interface {
	Ping(ctx context.Context) error
	Close() error

	QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)
	ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)

	Prepare(query string) (driver.Stmt, error)
	Begin() (driver.Tx, error)
	Commit() error
	Rollback() error
}

// httpDriver implements sql.Driver interface
type httpDriver struct{}

// Open returns new db connection
func (d *httpDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseHttpPDsn(dsn)
	if err != nil {
		return nil, err
	}

	return newHttpConn(cfg), nil
}

// httpConnOpener implements an interface sql.Conn
type httpConnOpener struct {
	url           *url.URL
	httpTransport *http.Transport
	location      *time.Location
	closed        int32
	stmts         []*httpBatch
}

func newHttpConn(cfg *httpConfig) *httpConnOpener {
	c := &httpConnOpener{
		httpTransport: newHttpTransport(cfg),
		location:      cfg.location,
		url:           buildUrl(cfg),
	}

	return c
}

func (c *httpConnOpener) CheckNamedValue(_ *driver.NamedValue) error { return nil }

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (c *httpConnOpener) Close() error {
	if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		httpTransport := c.httpTransport
		c.httpTransport = nil

		if httpTransport != nil {
			httpTransport.CloseIdleConnections()
		}
	}
	return nil
}
