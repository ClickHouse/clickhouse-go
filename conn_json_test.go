package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestDisableAutoFlattenedJSONOutputDSN(t *testing.T) {
	for _, value := range []string{"true", "false", "1", "0"} {
		opts, err := ParseDSN("clickhouse://localhost:9000?disable_auto_flattened_json_output=" + value)
		require.NoError(t, err)
		require.Equal(t, value == "true" || value == "1", opts.DisableAutoFlattenedJSONOutput)
		require.NotContains(t, opts.Settings, "disable_auto_flattened_json_output")
		require.Equal(t, !opts.DisableAutoFlattenedJSONOutput, opts.useFlattenedJSON(true, nil))
	}
	_, err := ParseDSN("clickhouse://localhost:9000?disable_auto_flattened_json_output=invalid")
	require.ErrorContains(t, err, "invalid disable_auto_flattened_json_output")
}

func TestJSONSerializationProbeSettings(t *testing.T) {
	require.False(t, supportsFlattenedJSON(proto.Version{Major: 25, Minor: 5}))
	for _, value := range []uint8{0, 1, 2} {
		block := proto.NewBlock()
		require.NoError(t, block.AddColumn("allowed", "UInt8"))
		require.NoError(t, block.Append(value))
		enabled, err := readJSONSerializationSettings(block)
		require.NoError(t, err)
		require.Equal(t, value == 1, enabled)
	}
	_, err := readJSONSerializationSettings(proto.NewBlock())
	require.Error(t, err)
}

type jsonProbeConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *jsonProbeConn) Read(p []byte) (int, error) {
	c.once.Do(func() {
		if c.entered != nil {
			close(c.entered)
		}
	})
	return c.Conn.Read(p)
}
func (c *jsonProbeConn) SetDeadline(time.Time) error     { return errors.New("deadline not supported") }
func (c *jsonProbeConn) SetReadDeadline(time.Time) error { return errors.New("deadline not supported") }

func newJSONProbeTestConn(t *testing.T, payload []byte) *connect {
	t.Helper()
	client, server := net.Pipe()
	drained := make(chan struct{})
	// The reader exits when cleanup closes the client socket.
	go func() {
		defer close(drained)
		if _, err := io.Copy(io.Discard, server); err != nil {
			t.Errorf("drain probe request: %v", err)
		}
	}()
	c := &connect{conn: client, reader: chproto.NewReader(bytes.NewReader(payload)), buffer: new(chproto.Buffer), opt: &Options{DialTimeout: time.Second}, readTimeout: time.Second, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	t.Cleanup(func() { require.NoError(t, c.close()); <-drained; require.NoError(t, server.Close()) })
	return c
}

func TestNativeJSONProbe(t *testing.T) {
	var payload chproto.Buffer
	payload.PutByte(proto.ServerException)
	payload.PutInt32(497)
	payload.PutString("ACCESS_DENIED")
	payload.PutString("not enough privileges")
	payload.PutString("")
	payload.PutBool(false)
	c := newJSONProbeTestConn(t, payload.Buf)
	c.conn = &jsonProbeConn{Conn: c.conn}
	require.NoError(t, c.probeJSONSerialization(context.Background()))
	require.False(t, c.flattenedJSON)
	require.False(t, c.isClosed())
	broken := newJSONProbeTestConn(t, []byte{proto.ServerException})
	require.ErrorContains(t, broken.probeJSONSerialization(context.Background()), "json settings probe")
}

func TestNativeJSONProbeExtremes(t *testing.T) {
	for _, complete := range []bool{true, false} {
		t.Run(fmt.Sprint(complete), func(t *testing.T) {
			var payload chproto.Buffer
			packets := []byte{proto.ServerData}
			if complete {
				packets = append(packets, proto.ServerExtremes)
			}
			for _, packet := range packets {
				block := proto.NewBlock()
				require.NoError(t, block.AddColumn("allowed", "UInt8"))
				require.NoError(t, block.Append(uint8(1)))
				if packet == proto.ServerExtremes {
					require.NoError(t, block.Append(uint8(1)))
				}
				payload.PutByte(packet)
				payload.PutString("")
				require.NoError(t, block.Encode(&payload, 0))
			}
			if complete {
				payload.PutByte(proto.ServerEndOfStream)
			}
			c := newJSONProbeTestConn(t, payload.Buf)
			err := c.probeJSONSerialization(context.Background())
			if complete {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "json settings probe")
			}
			require.Equal(t, complete, c.flattenedJSON)
		})
	}
}

func newHTTPJSONProbeTestConn(t *testing.T, handler http.HandlerFunc) *httpConnect {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	pool, err := createCompressionPool(&Compression{Method: CompressionNone})
	require.NoError(t, err)
	return &httpConnect{revision: ClientTCPProtocolVersion, url: u, client: server.Client(), opt: &Options{DialTimeout: time.Second}, compression: CompressionNone, compressionPool: pool, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestHTTPJSONProbe(t *testing.T) {
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("allowed", "UInt8"))
	require.NoError(t, block.Append(uint8(1)))
	var payload chproto.Buffer
	require.NoError(t, block.Encode(&payload, ClientTCPProtocolVersion))
	for _, tc := range []struct {
		name           string
		status         int
		body           []byte
		enabled, fails bool
	}{
		{"allowed", 200, payload.Buf, true, false},
		{"exception", 403, []byte("Code: 497. DB::Exception: not enough privileges"), false, false},
		{"in band exception", 200, []byte("__exception__\r\nCode: 497. DB::Exception: not enough privileges"), false, false},
		{"broken", 200, []byte{1}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHTTPJSONProbeTestConn(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Empty(t, r.URL.Query().Get("zz_no_such_setting"))
				assert.Empty(t, r.URL.Query().Get("max_threads"))
				assert.Empty(t, r.URL.Query().Get("log_comment"))
				assert.Equal(t, "json_probe_session", r.URL.Query().Get("session_id"))
				assert.Equal(t, "120", r.URL.Query().Get("session_timeout"))
				assert.Equal(t, "1", r.URL.Query().Get("session_check"))
				assert.Equal(t, "Native", r.URL.Query().Get("default_format"))
				assert.Equal(t, fmt.Sprint(ClientTCPProtocolVersion), r.URL.Query().Get("client_protocol_version"))
				if tc.name == "broken" {
					w.Header().Set("Content-Length", "100")
				}
				w.WriteHeader(tc.status)
				if _, err := w.Write(tc.body); err != nil {
					t.Errorf("write probe response: %v", err)
				}
			})
			h.opt.Settings = Settings{"zz_no_such_setting": 1, "max_threads": 3, "log_comment": "json_probe_marker", "session_id": "json_probe_session", "session_timeout": 120, "session_check": 1, "default_format": "JSONEachRow", "client_protocol_version": 0}
			q := h.url.Query()
			for key, value := range h.opt.Settings {
				q.Set(key, fmt.Sprint(value))
			}
			h.url.RawQuery = q.Encode()
			err := h.probeJSONSerialization(context.Background())
			if tc.fails {
				require.ErrorContains(t, err, "json settings probe")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.enabled, h.flattenedJSON)
		})
	}
}

func TestNativeJSONProbeExceptionKeepsDialConnection(t *testing.T) {
	const revision = proto.DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
	var hello, exception chproto.Buffer
	hello.PutByte(proto.ServerHello)
	hello.PutString("ClickHouse")
	hello.PutUVarInt(26)
	hello.PutUVarInt(8)
	hello.PutUVarInt(revision)
	hello.PutString("UTC")
	hello.PutString("probe server")
	hello.PutUVarInt(1)
	exception.PutByte(proto.ServerException)
	exception.PutInt32(497)
	exception.PutString("ACCESS_DENIED")
	exception.PutString("not enough privileges")
	exception.PutString("")
	exception.PutBool(false)
	client, server := net.Pipe()
	done := make(chan struct{})
	t.Cleanup(func() {
		require.NoError(t, client.Close())
		require.NoError(t, server.Close())
		<-done
	})
	// The server exits after the final response or when cleanup closes the pipe.
	go func() {
		defer close(done)
		request := make([]byte, 64*1024)
		for i, response := range [][]byte{hello.Buf, exception.Buf, {proto.ServerEndOfStream}} {
			n, err := server.Read(request)
			if !assert.NoError(t, err) {
				return
			}
			if i == 1 {
				assert.Contains(t, string(request[:n]), jsonSerializationProbe)
			}
			if i == 2 {
				assert.Contains(t, string(request[:n]), "SELECT 13")
				assert.NotContains(t, string(request[:n]), flattenedJSONSetting)
			}
			if _, err := server.Write(response); !assert.NoError(t, err) {
				return
			}
		}
	}()
	var logs bytes.Buffer
	opts := (&Options{
		DialTimeout: time.Second,
		Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
		DialContext: func(context.Context, string) (net.Conn, error) { return client, nil },
	}).setDefaults()
	c, err := dial(context.Background(), "probe server", 1, opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.close()) })
	require.False(t, c.isClosed())
	require.False(t, c.flattenedJSON)
	require.NoError(t, c.exec(context.Background(), "SELECT 13"))
	require.Contains(t, logs.String(), "level=WARN")
	require.Contains(t, logs.String(), flattenedJSONSetting)
	require.Contains(t, logs.String(), "DisableAutoFlattenedJSONOutput")
}

func TestHTTPJSONProbeExceptionKeepsDialConnection(t *testing.T) {
	hello := proto.NewBlock()
	require.NoError(t, hello.AddColumn("name", "String"))
	require.NoError(t, hello.AddColumn("version", "String"))
	require.NoError(t, hello.AddColumn("revision", "UInt32"))
	require.NoError(t, hello.AddColumn("timezone", "String"))
	require.NoError(t, hello.AddColumn("readonly", "UInt8"))
	require.NoError(t, hello.Append("probe server", "26.8.1", uint32(ClientTCPProtocolVersion), "UTC", uint8(0)))
	var payload chproto.Buffer
	require.NoError(t, hello.Encode(&payload, ClientTCPProtocolVersion))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, err := io.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		assert.False(t, r.URL.Query().Has(flattenedJSONSetting))
		switch {
		case strings.Contains(string(query), "displayName()"):
			_, err = w.Write(payload.Buf)
		case string(query) == jsonSerializationProbe:
			w.WriteHeader(http.StatusForbidden)
			_, err = io.WriteString(w, "Code: 497. DB::Exception: not enough privileges")
		default:
			assert.Equal(t, "SELECT 13", string(query))
		}
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	var logs bytes.Buffer
	opts := (&Options{Protocol: HTTP, Logger: slog.New(slog.NewTextHandler(&logs, nil))}).setDefaults()
	c, err := dialHttp(context.Background(), strings.TrimPrefix(server.URL, "http://"), 1, opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.close()) })
	require.NoError(t, c.healthCheck())
	require.False(t, c.flattenedJSON)
	require.NoError(t, c.exec(context.Background(), "SELECT 13"))
	require.Contains(t, logs.String(), "level=WARN")
	require.Contains(t, logs.String(), flattenedJSONSetting)
	require.Contains(t, logs.String(), "DisableAutoFlattenedJSONOutput")
}

func TestJSONProbeCancellation(t *testing.T) {
	for _, protocol := range []string{"Native", "HTTP"} {
		t.Run(protocol, func(t *testing.T) {
			entered := make(chan struct{})
			var probe func(context.Context) error
			if protocol == "Native" {
				c := newJSONProbeTestConn(t, nil)
				socket := &jsonProbeConn{Conn: c.conn, entered: entered}
				c.conn, c.reader, probe = socket, chproto.NewReader(socket), c.probeJSONSerialization
			} else {
				h := newHTTPJSONProbeTestConn(t, func(w http.ResponseWriter, r *http.Request) {
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Errorf("read request: %v", err)
					}
					close(entered)
					<-r.Context().Done()
				})
				probe = h.probeJSONSerialization
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			// The probe exits when cancellation stops socket I/O.
			go func() { done <- probe(ctx) }()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("probe did not start")
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
				require.ErrorContains(t, err, "json settings probe")
			case <-time.After(2 * time.Second):
				t.Fatal("probe did not stop")
			}
		})
	}
}

func TestHTTPJSONSerializationRecovery(t *testing.T) {
	for _, tc := range []struct {
		name              string
		code              int32
		message, explicit string
		disabled          bool
	}{
		{"readonly", 164, flattenedJSONSetting, "", true},
		{"constraint", 452, flattenedJSONSetting, "", true},
		{"unrelated readonly", 164, "Cannot execute query in readonly mode", "", false},
		{"unrelated constraint", 452, "max_memory_usage exceeds maximum", "", false},
		{"connection", 164, flattenedJSONSetting, "connection", false},
		{"query", 452, flattenedJSONSetting, "query", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHTTPJSONProbeTestConn(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(exceptionCodeHeader, fmt.Sprint(tc.code))
				w.WriteHeader(500)
				if _, err := fmt.Fprintf(w, "Code: %d. DB::Exception: %s", tc.code, tc.message); err != nil {
					t.Errorf("write exception: %v", err)
				}
			})
			h.flattenedJSON = true
			ctx := context.Background()
			if tc.explicit == "connection" {
				h.opt.Settings = Settings{flattenedJSONSetting: 1}
			}
			if tc.explicit == "query" {
				ctx = Context(ctx, WithSettings(Settings{flattenedJSONSetting: 1}))
			}
			err := h.exec(ctx, "SELECT 1")
			var ex *Exception
			require.ErrorAs(t, err, &ex)
			require.Equal(t, tc.code, ex.Code)
			require.Equal(t, tc.message, ex.Message)
			require.Equal(t, tc.disabled, h.jsonSerialization.disabled.Load())
			req, err := h.createRequest(context.Background(), h.url.String(), nil, &QueryOptions{}, nil)
			require.NoError(t, err)
			require.Equal(t, !tc.disabled && tc.explicit != "connection", req.URL.Query().Has(flattenedJSONSetting))
		})
	}
}

func TestHTTPJSONRawFormats(t *testing.T) {
	h := newHTTPJSONProbeTestConn(t, func(w http.ResponseWriter, r *http.Request) {
		assert.False(t, r.URL.Query().Has(flattenedJSONSetting))
		if _, err := io.WriteString(w, "payload"); err != nil {
			t.Errorf("write response: %v", err)
		}
	})
	h.flattenedJSON = true
	stream, err := h.queryFormat(context.Background(), func(nativeTransport, error) {}, "Native", "SELECT 1")
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.NoError(t, h.insertFormat(context.Background(), func(nativeTransport, error) {}, "Native", "INSERT INTO t", strings.NewReader("payload")))
}
