package clickhouse

import (
	"context"
	"runtime"
	"strings"
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

// newWatchdogBatchConnect builds a connect whose writes succeed and whose
// reads end the query with a server EndOfStream packet.
func newWatchdogBatchConnect() (*connect, *mockNetConn) {
	mock := &mockNetConn{}
	eos := &endOfStreamConn{mockNetConn: mock}
	return &connect{
		id:                   1,
		conn:                 eos,
		buffer:               new(chproto.Buffer),
		reader:               chproto.NewReader(eos),
		connectedAt:          time.Now(),
		readTimeout:          10 * time.Second,
		compression:          CompressionNone,
		maxCompressionBuffer: 1024 * 1024,
		logger:               newNoopLogger(),
		opt:                  &Options{},
		revision:             ClientTCPProtocolVersion,
	}, mock
}

// lateWatchdogCtx delays Done() only when it is read from the goroutine that
// contextWatchdog starts. It models a watchdog goroutine that has not reached
// its select when Send stops the watchdog: by the time the delayed Done()
// returns, both the stop channel and the context Done are ready, so the select
// picks one at random and the callback can still run.
type lateWatchdogCtx struct {
	context.Context
	delay time.Duration
}

func (c lateWatchdogCtx) Done() <-chan struct{} {
	pcs := make([]uintptr, 16)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "contextWatchdog") {
			time.Sleep(c.delay)
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Done()
}

// TestBatchSendWatchdogClosedConnNotReleasedAsHealthy drives batch.Send with a
// context whose deadline ends after Send's work has succeeded but before Send
// stops the watchdog. In that window the watchdog callback can still run and
// close the connection. Send must then release the closed connection with a
// non-nil error, because release() with a nil error returns the connection to
// the pool as healthy. See https://github.com/ClickHouse/clickhouse-go/issues/2076.
func TestBatchSendWatchdogClosedConnNotReleasedAsHealthy(t *testing.T) {
	const rounds = 40
	for i := 0; i < rounds; i++ {
		c, mock := newWatchdogBatchConnect()
		innerCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		ctx := lateWatchdogCtx{Context: innerCtx, delay: 60 * time.Millisecond}

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

		err := b.Send()
		cancel()

		// A connection released without an error goes back to the pool as
		// healthy, so it must not have been closed by the watchdog.
		if releasedErr == nil && mock.closed {
			t.Fatalf("round %d: released a watchdog-closed connection to the pool without an error (send err: %v)", i, err)
		}
		if releasedErr != nil {
			require.ErrorIs(t, releasedErr, context.DeadlineExceeded, "round %d: expected the context error on a watchdog-closed connection", i)
		}
	}
}
