package clickhouse

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/compress"
	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockNetConn is a mock net.Conn that can be configured to return specific errors
type mockNetConn struct {
	net.Conn
	readErr  error
	writeErr error
	closed   bool
}

func (m *mockNetConn) Read(b []byte) (n int, err error) {
	if m.readErr != nil {
		return 0, m.readErr
	}
	return 0, io.EOF
}

func (m *mockNetConn) Write(b []byte) (n int, err error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	return len(b), nil
}

func (m *mockNetConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockNetConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}

func (m *mockNetConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9000}
}

func (m *mockNetConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockNetConn) SetWriteDeadline(t time.Time) error { return nil }

// deadlineRecorderConn wraps a net.Conn and records SetDeadline and SetReadDeadline calls for tests.
type deadlineRecorderConn struct {
	net.Conn
	setDeadlineCalls     []time.Time
	setReadDeadlineCalls []time.Time
}

func (d *deadlineRecorderConn) SetDeadline(t time.Time) error {
	d.setDeadlineCalls = append(d.setDeadlineCalls, t)
	return d.Conn.SetDeadline(t)
}

func (d *deadlineRecorderConn) SetReadDeadline(t time.Time) error {
	d.setReadDeadlineCalls = append(d.setReadDeadlineCalls, t)
	return d.Conn.SetReadDeadline(t)
}

func (d *deadlineRecorderConn) SetWriteDeadline(t time.Time) error {
	return d.Conn.SetWriteDeadline(t)
}

// createMockConnect creates a connect instance with mock components for testing
func createMockConnect(mockConn net.Conn) *connect {
	reader := chproto.NewReader(mockConn)
	buffer := new(chproto.Buffer)
	compressor := &compress.Writer{}

	return &connect{
		id:                   1,
		conn:                 mockConn,
		buffer:               buffer,
		reader:               reader,
		connectedAt:          time.Now().Add(-5 * time.Minute),
		readTimeout:          10 * time.Second,
		compression:          CompressionLZ4,
		compressor:           compressor,
		maxCompressionBuffer: 1024 * 1024,
		logger:               newNoopLogger(),
		opt:                  &Options{},
		revision:             ClientTCPProtocolVersion,
	}
}

// TestHandshakeErrorContext tests that handshake errors include server address and connection info
func TestHandshakeErrorContext(t *testing.T) {
	mockConn := &mockNetConn{readErr: io.EOF}
	conn := createMockConnect(mockConn)

	err := conn.handshake(context.Background(), Auth{
		Database: "default",
		Username: "default",
		Password: "",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "handshake")
	assert.Contains(t, err.Error(), "127.0.0.1:9000", "should contain server address")
	assert.Contains(t, err.Error(), "conn_id=1", "should contain connection ID")
	assert.Contains(t, err.Error(), "auth_db=default", "should contain database name")
	assert.True(t, errors.Is(err, io.EOF), "should wrap io.EOF")
}

// TestQueryProcessingErrorContext tests that query processing errors include connection info
func TestQueryProcessingErrorContext(t *testing.T) {
	mockConn := &mockNetConn{readErr: io.EOF}
	conn := createMockConnect(mockConn)

	_, err := conn.firstBlockImpl(context.Background(), &onProcess{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query processing")
	assert.Contains(t, err.Error(), "127.0.0.1:9000", "should contain server address")
	assert.Contains(t, err.Error(), "conn_id=1", "should contain connection ID")
	assert.True(t, errors.Is(err, io.EOF), "should wrap io.EOF")
}

// TestPingErrorContext tests that ping errors include connection age and connection info
func TestPingErrorContext(t *testing.T) {
	mockConn := &mockNetConn{readErr: io.EOF}
	conn := createMockConnect(mockConn)

	// First flush succeeds (mocked Write), then read fails
	mockConn.writeErr = nil // Allow flush to succeed
	err := conn.ping(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ping")
	assert.Contains(t, err.Error(), "127.0.0.1:9000", "should contain server address")
	assert.Contains(t, err.Error(), "conn_id=1", "should contain connection ID")
	assert.Contains(t, err.Error(), "age=", "should contain connection age")
	assert.True(t, errors.Is(err, io.EOF), "should wrap io.EOF")
}

// TestReadDataErrorContext tests that read data errors include connection and compression info
func TestReadDataErrorContext(t *testing.T) {
	mockConn := &mockNetConn{readErr: io.EOF}
	conn := createMockConnect(mockConn)

	_, err := conn.readData(context.Background(), proto.ServerData, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "read data")
	assert.Contains(t, err.Error(), "127.0.0.1:9000", "should contain server address")
	assert.Contains(t, err.Error(), "conn_id=1", "should contain connection ID")
	assert.True(t, errors.Is(err, io.EOF), "should wrap io.EOF")
}

// TestSendDataErrorContext tests that send data errors include block information
func TestSendDataErrorContext(t *testing.T) {
	mockConn := &mockNetConn{writeErr: io.EOF}
	conn := createMockConnect(mockConn)

	// Create a simple block with columns for testing
	block := proto.NewBlock()
	_ = block.AddColumn("col1", "UInt64")
	_ = block.AddColumn("col2", "String")

	err := conn.sendData(block, "test")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "send data")
	assert.Contains(t, err.Error(), "127.0.0.1:9000", "should contain server address")
	assert.Contains(t, err.Error(), "conn_id=1", "should contain connection ID")
	assert.Contains(t, err.Error(), "block_cols=2", "should contain block column count")
	assert.Contains(t, err.Error(), "block_rows=", "should contain block row count")
	assert.True(t, errors.Is(err, io.EOF), "should wrap io.EOF")
}

// TestErrorContextPreservesEOF tests that all error wrappers preserve io.EOF for errors.Is
func TestErrorContextPreservesEOF(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(*connect) error
	}{
		{
			name: "handshake",
			testFunc: func(c *connect) error {
				return c.handshake(context.Background(), Auth{Database: "default"})
			},
		},
		{
			name: "ping",
			testFunc: func(c *connect) error {
				return c.ping(context.Background())
			},
		},
		{
			name: "firstBlock",
			testFunc: func(c *connect) error {
				_, err := c.firstBlockImpl(context.Background(), &onProcess{})
				return err
			},
		},
		{
			name: "readData",
			testFunc: func(c *connect) error {
				_, err := c.readData(context.Background(), proto.ServerData, false)
				return err
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockConn := &mockNetConn{readErr: io.EOF}
			conn := createMockConnect(mockConn)

			err := tc.testFunc(conn)

			require.Error(t, err)
			assert.True(t, errors.Is(err, io.EOF),
				"error from %s should preserve io.EOF for errors.Is check", tc.name)

			// Also verify the error is not bare io.EOF
			assert.NotEqual(t, io.EOF, err,
				"error from %s should be wrapped, not bare io.EOF", tc.name)

			// Verify error message has context
			assert.NotEqual(t, "EOF", err.Error(),
				"error from %s should have context beyond just 'EOF'", tc.name)
		})
	}
}

// TestErrorContextDistinguishesOperations tests that different operations produce distinguishable errors
func TestErrorContextDistinguishesOperations(t *testing.T) {
	mockConn := &mockNetConn{readErr: io.EOF}

	operations := map[string]func(*connect) error{
		"handshake": func(c *connect) error {
			return c.handshake(context.Background(), Auth{Database: "default"})
		},
		"ping": func(c *connect) error {
			return c.ping(context.Background())
		},
		"query processing": func(c *connect) error {
			_, err := c.firstBlockImpl(context.Background(), &onProcess{})
			return err
		},
		"read data": func(c *connect) error {
			_, err := c.readData(context.Background(), proto.ServerData, false)
			return err
		},
	}

	errorMessages := make(map[string]string)

	for opName, opFunc := range operations {
		conn := createMockConnect(mockConn)
		err := opFunc(conn)
		require.Error(t, err, "operation %s should return error", opName)
		errorMessages[opName] = err.Error()
	}

	// Verify all error messages are unique and contain the operation name
	for opName, errMsg := range errorMessages {
		assert.Contains(t, strings.ToLower(errMsg), strings.ToLower(opName),
			"error message should identify the operation: %s", opName)

		// Verify this error is different from all others
		for otherOp, otherMsg := range errorMessages {
			if opName != otherOp {
				assert.NotEqual(t, errMsg, otherMsg,
					"error messages for %s and %s should be different", opName, otherOp)
			}
		}
	}
}

// TestIsConnBrokenErrorDetectsEOF tests that isConnBrokenError still detects EOF errors
func TestIsConnBrokenErrorDetectsEOF(t *testing.T) {
	mockConn := &mockNetConn{readErr: io.EOF}
	conn := createMockConnect(mockConn)

	// Test various wrapped EOF errors
	testCases := []struct {
		name     string
		getError func(*connect) error
	}{
		{
			name: "handshake EOF",
			getError: func(c *connect) error {
				return c.handshake(context.Background(), Auth{Database: "default"})
			},
		},
		{
			name: "ping EOF",
			getError: func(c *connect) error {
				return c.ping(context.Background())
			},
		},
		{
			name: "query processing EOF",
			getError: func(c *connect) error {
				_, err := c.firstBlockImpl(context.Background(), &onProcess{})
				return err
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.getError(conn)
			require.Error(t, err)

			// Verify that isConnBrokenError correctly identifies these as connection errors
			isBroken := isConnBrokenError(err)
			assert.True(t, isBroken,
				"isConnBrokenError should detect wrapped EOF from %s", tc.name)
		})
	}
}

// TestHandshakeRespectsContextDeadline verifies that handshake sets the connection deadline from context when ctx has a deadline.
func TestHandshakeRespectsContextDeadline(t *testing.T) {
	baseConn := &mockNetConn{readErr: io.EOF}
	recorder := &deadlineRecorderConn{Conn: baseConn}
	conn := createMockConnect(recorder)

	ctxDeadline := time.Now().Add(30 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), ctxDeadline)
	defer cancel()

	_ = conn.handshake(ctx, Auth{Database: "default", Username: "default", Password: ""})

	// handshake sets read deadline, then if ctx has deadline it sets conn deadline, then on return defers clear.
	// So we expect at least one SetDeadline call with the context deadline, and one with zero to clear.
	require.NotEmpty(t, recorder.setDeadlineCalls, "SetDeadline should have been called when context has deadline")
	// First SetDeadline(deadline) should be the context deadline (within a small tolerance)
	first := recorder.setDeadlineCalls[0]
	assert.WithinDuration(t, ctxDeadline, first, time.Millisecond,
		"first SetDeadline should use context deadline")
	// Last call should be clear (zero time)
	last := recorder.setDeadlineCalls[len(recorder.setDeadlineCalls)-1]
	assert.True(t, last.IsZero(), "last SetDeadline should be zero to clear deadline")
}

// TestHandshakeNoContextDeadline verifies that handshake does not call SetDeadline when context has no deadline.
func TestHandshakeNoContextDeadline(t *testing.T) {
	baseConn := &mockNetConn{readErr: io.EOF}
	recorder := &deadlineRecorderConn{Conn: baseConn}
	conn := createMockConnect(recorder)

	_ = conn.handshake(context.Background(), Auth{Database: "default", Username: "default", Password: ""})

	// With context.Background(), ctx.Deadline() returns (zero, false), so SetDeadline is never called from the context path.
	assert.Empty(t, recorder.setDeadlineCalls,
		"SetDeadline should not be called when context has no deadline")
	// SetReadDeadline is still called
	require.NotEmpty(t, recorder.setReadDeadlineCalls,
		"SetReadDeadline should still be called")
}
