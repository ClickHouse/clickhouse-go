package clickhouse

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"
)

// endOfStreamConn wraps mockNetConn so reads return a server EndOfStream
// packet. The process() call in closeQuery() then succeeds without a server.
type endOfStreamConn struct {
	*mockNetConn
}

func (e *endOfStreamConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	b[0] = proto.ServerEndOfStream
	return 1, nil
}

// gatedEndOfStreamConn parks the read that closeQuery's process() performs
// until the test opens readGate, and reports its first Close() on
// closedSignal. Like a real net.Conn, Close() works while the connection is
// in use, and like the mock it lets Read() still succeed afterwards, which is
// what lets closeQuery finish successfully even though the watchdog closed
// the connection underneath Send.
type gatedEndOfStreamConn struct {
	*endOfStreamConn
	readStarted  chan struct{} // closed when Read reaches the gate
	readGate     chan struct{} // the test closes it to let Read finish
	closedSignal chan struct{} // closed on the first Close
	readOnce     sync.Once
	closeOnce    sync.Once
}

func (g *gatedEndOfStreamConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	g.readOnce.Do(func() { close(g.readStarted) })
	<-g.readGate
	return g.endOfStreamConn.Read(b)
}

func (g *gatedEndOfStreamConn) Close() error {
	err := g.endOfStreamConn.Close()
	g.closeOnce.Do(func() { close(g.closedSignal) })
	return err
}

// newGatedWatchdogBatchConnect builds a connect whose writes succeed, whose
// single read (the one closeQuery performs) parks on a gate, and whose Close()
// is observable by the test.
func newGatedWatchdogBatchConnect() (*connect, *mockNetConn, *gatedEndOfStreamConn) {
	mock := &mockNetConn{}
	gate := &gatedEndOfStreamConn{
		endOfStreamConn: &endOfStreamConn{mockNetConn: mock},
		readStarted:     make(chan struct{}),
		readGate:        make(chan struct{}),
		closedSignal:    make(chan struct{}),
	}
	return &connect{
		id:                   1,
		conn:                 gate,
		buffer:               new(chproto.Buffer),
		reader:               chproto.NewReader(gate),
		connectedAt:          time.Now(),
		readTimeout:          10 * time.Second,
		compression:          CompressionNone,
		maxCompressionBuffer: 1024 * 1024,
		logger:               newNoopLogger(),
		opt:                  &Options{},
		revision:             ClientTCPProtocolVersion,
	}, mock, gate
}

// inContextWatchdogGoroutine reports whether the caller runs on the goroutine
// that contextWatchdog starts for its select and callback.
func inContextWatchdogGoroutine() bool {
	pcs := make([]uintptr, 16)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "contextWatchdog") {
			return true
		}
		if !more {
			return false
		}
	}
}

// watchdogGateCtx is a test context that makes the batch watchdog
// interleaving deterministic. Done() behaves differently per caller:
//
//   - Called from the contextWatchdog goroutine (detected by stack frame, the
//     same trick the integration tests' lateWatchdogCtx uses), it signals
//     watchdogParked and then blocks until the test closes releaseWatchdog.
//     This parks the watchdog goroutine before it can reach its select. The
//     call then returns watchdogDone, which the test closes so the select has
//     exactly one ready case and the callback runs deterministically.
//   - Called from anywhere else (the selects in connect.process), it returns
//     othersDone, which is never closed. That is what removes the last race:
//     connect.process's select never observes the context as done, so
//     closeQuery can still succeed after the watchdog closed the connection,
//     and Send's outcome does not depend on a select choosing at random
//     between two ready cases.
//
// Err() reports the error the test set at cancel time, so the fix's deferred
// release maps the watchdog close to a real context error.
type watchdogGateCtx struct {
	context.Context

	mu     sync.Mutex
	errVal error

	watchdogParked  chan struct{}
	parkOnce        sync.Once
	releaseWatchdog chan struct{}
	watchdogDone    chan struct{}
	othersDone      chan struct{}
}

func newWatchdogGateCtx() *watchdogGateCtx {
	return &watchdogGateCtx{
		Context:         context.Background(),
		watchdogParked:  make(chan struct{}),
		releaseWatchdog: make(chan struct{}),
		watchdogDone:    make(chan struct{}),
		othersDone:      make(chan struct{}),
	}
}

func (c *watchdogGateCtx) Done() <-chan struct{} {
	if !inContextWatchdogGoroutine() {
		return c.othersDone
	}
	c.parkOnce.Do(func() { close(c.watchdogParked) })
	<-c.releaseWatchdog
	return c.watchdogDone
}

func (c *watchdogGateCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errVal
}

// cancel reports err as the context error, closes watchdogDone so the parked
// watchdog select fires, and releases the parked Done() call.
func (c *watchdogGateCtx) cancel(err error) {
	c.mu.Lock()
	c.errVal = err
	c.mu.Unlock()
	close(c.watchdogDone)
	close(c.releaseWatchdog)
}

// waitSignal fails the test if ch is not closed within 5 seconds. The bound
// keeps a broken rig from hanging the whole suite; on the happy path it adds
// no delay because every ordering step is signalled directly, so there are no
// sleeps.
func waitSignal(t *testing.T, ch chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

// TestBatchSendWatchdogClosedConnNotReleasedAsHealthy forces, instead of
// racing for it, the interleaving from
// https://github.com/ClickHouse/clickhouse-go/issues/2076: the batch context
// ends while Send is still inside closeQuery, the watchdog callback then
// closes the connection, and closeQuery still succeeds because the mock keeps
// Read working after Close. Send therefore finishes while the connection was
// closed under it, and must not release it to the pool as healthy.
//
// The interleaving is pinned step by step:
//
//  1. The watchdog goroutine parks inside its Done() call, before it can
//     reach its select (watchdogParked).
//  2. closeQuery's read parks on the connection gate (readStarted).
//  3. The test cancels the context and releases the watchdog. The watchdog's
//     select then has exactly one ready case, because stop is not closed yet
//     (Send is still blocked in Read), so the callback runs
//     deterministically.
//  4. The test confirms the callback closed the connection (closedSignal),
//     then lets the read finish with a server EndOfStream so Send returns
//     without an error.
//
// The pool invariant asserted at the end fails on code without the
// conn_batch.go fix.
func TestBatchSendWatchdogClosedConnNotReleasedAsHealthy(t *testing.T) {
	c, mock, gate := newGatedWatchdogBatchConnect()
	ctx := newWatchdogGateCtx()

	var releasedErr error
	b := &batch{
		ctx:       ctx,
		conn:      c,
		block:     proto.NewBlock(),
		onProcess: &onProcess{},
		connRelease: func(conn *connect, err error) {
			releasedErr = err
		},
	}

	sendErrCh := make(chan error, 1)
	go func() { sendErrCh <- b.Send() }()

	// Steps 1 and 2: park the watchdog goroutine and park Send inside
	// closeQuery's read.
	waitSignal(t, ctx.watchdogParked, "contextWatchdog goroutine never reached its Done() call")
	waitSignal(t, gate.readStarted, "closeQuery never reached the gated read")

	// Step 3: cancel while both are parked. The watchdog's select has exactly
	// one ready case, so the callback runs and closes the connection.
	ctx.cancel(context.Canceled)

	// Step 4: the callback closed the connection under Send.
	waitSignal(t, gate.closedSignal, "watchdog callback never closed the connection")

	// Let the read finish with a server EndOfStream so closeQuery succeeds
	// and Send returns without an error.
	close(gate.readGate)

	var sendErr error
	select {
	case sendErr = <-sendErrCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("Send never returned (release err: %v)", releasedErr)
	}

	// Pool invariant: a connection released with a nil error goes back to the
	// pool as healthy, so the watchdog must never have closed it.
	if releasedErr == nil && mock.closed {
		t.Fatalf("released a watchdog-closed connection to the pool without an error (send err: %v)", sendErr)
	}

	// With the fix: Send succeeded, but the watchdog had closed the
	// connection, so Send reports the context error and release() receives
	// it, which makes the pool discard the connection instead of pooling it.
	require.ErrorIs(t, sendErr, context.Canceled, "Send must report the context error when the watchdog closed the connection")
	require.ErrorIs(t, releasedErr, context.Canceled, "release must receive the context error so the pool discards the watchdog-closed connection")
}
