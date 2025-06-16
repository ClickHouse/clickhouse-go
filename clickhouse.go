// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	_ "time/tzdata"

	"github.com/ClickHouse/clickhouse-go/v2/contributors"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type Conn = driver.Conn

type (
	Progress      = proto.Progress
	Exception     = proto.Exception
	ProfileInfo   = proto.ProfileInfo
	ServerVersion = proto.ServerHandshake
)

var (
	ErrBatchInvalid              = errors.New("clickhouse: batch is invalid. check appended data is correct")
	ErrBatchAlreadySent          = errors.New("clickhouse: batch has already been sent")
	ErrBatchNotSent              = errors.New("clickhouse: invalid retry, batch not sent yet")
	ErrAcquireConnTimeout        = errors.New("clickhouse: acquire conn timeout. you can increase the number of max open conn or the dial timeout")
	ErrUnsupportedServerRevision = errors.New("clickhouse: unsupported server revision")
	ErrBindMixedParamsFormats    = errors.New("clickhouse [bind]: mixed named, numeric or positional parameters")
	ErrAcquireConnNoAddress      = errors.New("clickhouse: no valid address supplied")
	ErrServerUnexpectedData      = errors.New("code: 101, message: Unexpected packet Data received from client")
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
	if opt == nil {
		opt = &Options{}
	}
	o := opt.setDefaults()

	conn := &clickhouse{
		opt:  o,
		idle: make(chan nativeTransport, o.MaxIdleConns),
		open: make(chan struct{}, o.MaxOpenConns),
		exit: make(chan struct{}),
	}
	go conn.startAutoCloseIdleConnections()
	return conn, nil
}

// nativeTransport represents an implementation (TCP or HTTP) that can be pooled by the main clickhouse struct.
// Implementations are not expected to be thread safe, which is why we provide acquire/release functions.
type nativeTransport interface {
	serverVersion() (*ServerVersion, error)
	query(ctx context.Context, release nativeTransportRelease, query string, args ...any) (*rows, error)
	queryRow(ctx context.Context, release nativeTransportRelease, query string, args ...any) *row
	prepareBatch(ctx context.Context, release nativeTransportRelease, acquire nativeTransportAcquire, query string, opts driver.PrepareBatchOptions) (driver.Batch, error)
	exec(ctx context.Context, query string, args ...any) error
	asyncInsert(ctx context.Context, query string, wait bool, args ...any) error
	ping(context.Context) error
	isBad() bool
	connID() int
	connectedAtTime() time.Time
	isReleased() bool
	setReleased(released bool)
	debugf(format string, v ...any)
	// freeBuffer is called if Options.FreeBufOnConnRelease is set
	freeBuffer()
	close() error
}
type nativeTransportAcquire func(context.Context) (nativeTransport, error)
type nativeTransportRelease func(nativeTransport, error)

type clickhouse struct {
	opt    *Options
	idle   chan nativeTransport
	open   chan struct{}
	exit   chan struct{}
	connID int64
}

func (clickhouse) Contributors() []string {
	list := contributors.List
	if len(list[len(list)-1]) == 0 {
		return list[:len(list)-1]
	}
	return list
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
	defer ch.release(conn, nil)
	return conn.serverVersion()
}

func (ch *clickhouse) Query(ctx context.Context, query string, args ...any) (rows driver.Rows, err error) {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return nil, err
	}
	conn.debugf("[query] \"%s\"", query)
	return conn.query(ctx, ch.release, query, args...)
}

func (ch *clickhouse) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return &row{
			err: err,
		}
	}

	conn.debugf("[query row] \"%s\"", query)
	return conn.queryRow(ctx, ch.release, query, args...)
}

func (ch *clickhouse) Exec(ctx context.Context, query string, args ...any) error {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return err
	}
	conn.debugf("[exec] \"%s\"", query)
	if err := conn.exec(ctx, query, args...); err != nil {
		ch.release(conn, err)
		return err
	}
	ch.release(conn, nil)
	return nil
}

func (ch *clickhouse) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return nil, err
	}
	conn.debugf("[prepare batch] \"%s\"", query)
	batch, err := conn.prepareBatch(ctx, ch.release, ch.acquire, query, getPrepareBatchOptions(opts...))
	if err != nil {
		return nil, err
	}
	return batch, nil
}

func getPrepareBatchOptions(opts ...driver.PrepareBatchOption) driver.PrepareBatchOptions {
	var options driver.PrepareBatchOptions

	for _, opt := range opts {
		opt(&options)
	}

	return options
}

func (ch *clickhouse) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return err
	}
	conn.debugf("[async insert] \"%s\"", query)
	if err := conn.asyncInsert(ctx, query, wait, args...); err != nil {
		ch.release(conn, err)
		return err
	}
	ch.release(conn, nil)
	return nil
}

func (ch *clickhouse) Ping(ctx context.Context) (err error) {
	conn, err := ch.acquire(ctx)
	if err != nil {
		return err
	}
	conn.debugf("[ping]")
	if err := conn.ping(ctx); err != nil {
		ch.release(conn, err)
		return err
	}
	ch.release(conn, nil)
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

func (ch *clickhouse) dial(ctx context.Context) (conn nativeTransport, err error) {
	connID := int(atomic.AddInt64(&ch.connID, 1))

	dialFunc := func(ctx context.Context, addr string, opt *Options) (DialResult, error) {
		var conn nativeTransport
		var err error
		switch opt.Protocol {
		case HTTP:
			conn, err = dialHttp(ctx, addr, connID, opt)
		default:
			conn, err = dial(ctx, addr, connID, opt)
		}

		return DialResult{conn}, err
	}

	dialStrategy := DefaultDialStrategy
	if ch.opt.DialStrategy != nil {
		dialStrategy = ch.opt.DialStrategy
	}

	result, err := dialStrategy(ctx, connID, ch.opt, dialFunc)
	if err != nil {
		return nil, err
	}
	return result.conn, nil
}

func DefaultDialStrategy(ctx context.Context, connID int, opt *Options, dial Dial) (r DialResult, err error) {
	for i := range opt.Addr {
		var num int
		switch opt.ConnOpenStrategy {
		case ConnOpenInOrder:
			num = i
		case ConnOpenRoundRobin:
			num = (connID + i) % len(opt.Addr)
		case ConnOpenRandom:
			random := rand.Int()
			num = (random + i) % len(opt.Addr)
		}

		if r, err = dial(ctx, opt.Addr[num], opt); err == nil {
			return r, nil
		}
	}

	if err == nil {
		err = ErrAcquireConnNoAddress
	}

	return r, err
}

func (ch *clickhouse) acquire(ctx context.Context) (conn nativeTransport, err error) {
	timer := time.NewTimer(ch.opt.DialTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	select {
	case <-timer.C:
		return nil, ErrAcquireConnTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	case ch.open <- struct{}{}:
	}
	select {
	case <-timer.C:
		select {
		case <-ch.open:
		default:
		}
		return nil, ErrAcquireConnTimeout
	case conn := <-ch.idle:
		if conn.isBad() {
			conn.close()
			if conn, err = ch.dial(ctx); err != nil {
				select {
				case <-ch.open:
				default:
				}
				return nil, err
			}
		}
		conn.setReleased(false)
		conn.debugf("[acquired from pool]")
		return conn, nil
	default:
	}
	if conn, err = ch.dial(ctx); err != nil {
		select {
		case <-ch.open:
		default:
		}
		return nil, err
	}
	conn.debugf("[acquired new]")
	return conn, nil
}

func (ch *clickhouse) startAutoCloseIdleConnections() {
	ticker := time.NewTicker(ch.opt.ConnMaxLifetime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ch.closeIdleExpired()
		case <-ch.exit:
			return
		}
	}
}

func (ch *clickhouse) closeIdleExpired() {
	cutoff := time.Now().Add(-ch.opt.ConnMaxLifetime)
	for {
		select {
		case conn := <-ch.idle:
			if conn.connectedAtTime().Before(cutoff) {
				conn.close()
			} else {
				select {
				case ch.idle <- conn:
				default:
					conn.close()
				}
				return
			}
		default:
			return
		}
	}
}

func (ch *clickhouse) release(conn nativeTransport, err error) {
	if conn.isReleased() {
		return
	}
	conn.setReleased(true)

	if err != nil {
		conn.debugf("[released with error]")
	} else {
		conn.debugf("[released]")
	}

	select {
	case <-ch.open:
	default:
	}

	if err != nil {
		conn.debugf("[close: error] %s", err.Error())
		conn.close()
		return
	} else if time.Since(conn.connectedAtTime()) >= ch.opt.ConnMaxLifetime {
		conn.debugf("[close: lifetime expired]")
		conn.close()
		return
	}

	if ch.opt.FreeBufOnConnRelease {
		conn.debugf("[free buffer]")
		conn.freeBuffer()
	}

	select {
	case ch.idle <- conn:
	default:
		conn.debugf("[close: idle pool full %d/%d]", len(ch.idle), cap(ch.idle))
		conn.close()
	}
}

func (ch *clickhouse) Close() error {
	for {
		select {
		case conn := <-ch.idle:
			conn.debugf("[close: closing pool]")
			conn.close()
		default:
			// In rare cases, close may be called multiple times, don't block
			//TODO: add proper close flag to indicate this pool is unusable after Close
			select {
			case ch.exit <- struct{}{}:
			default:
			}
			return nil
		}
	}
}
