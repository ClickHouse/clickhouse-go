package clickhouse

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	chproto "github.com/ClickHouse/ch-go/proto"

	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// helloQuery is the internal server-info statement. A request is the handshake
// when its body carries this text, including a multipart body.
const helloQuery = "SELECT displayName(), version(), revision(), timezone()"

type capturedHTTPQuery struct {
	body  string
	query url.Values
}

// serveHTTPHello records every request. The handshake is answered with a Native
// block whose timezone() follows the session_timezone query parameter, which is
// what the server does. Any other statement is answered as CSV.
func serveHTTPHello(t *testing.T) (*httptest.Server, *[]capturedHTTPQuery) {
	t.Helper()
	var mu sync.Mutex
	got := make([]capturedHTTPQuery, 0, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		q := r.URL.Query()
		mu.Lock()
		got = append(got, capturedHTTPQuery{body: string(raw), query: q})
		mu.Unlock()

		if strings.Contains(string(raw), "displayName()") {
			tz := q.Get("session_timezone")
			if tz == "" {
				tz = "UTC"
			}
			body, err := encodeNativeHello(tz)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = w.Write(body)
			return
		}
		_, _ = io.WriteString(w, "1\n")
	}))
	t.Cleanup(srv.Close)
	return srv, &got
}

func encodeNativeHello(tz string) ([]byte, error) {
	block := proto.NewBlock()
	for _, col := range []struct {
		name string
		typ  column.Type
	}{
		{"displayName()", column.Type("String")},
		{"version()", column.Type("String")},
		{"revision()", column.Type("UInt32")},
		{"timezone()", column.Type("String")},
	} {
		if err := block.AddColumn(col.name, col.typ); err != nil {
			return nil, err
		}
	}
	if err := block.Append("test", "26.9.1.1", uint32(1), tz); err != nil {
		return nil, err
	}
	var buf chproto.Buffer
	if err := block.Encode(&buf, uint64(ClientTCPProtocolVersion)); err != nil {
		return nil, err
	}
	return append([]byte(nil), buf.Buf...), nil
}

func openHTTP(t *testing.T, addr string, settings Settings) driverConn {
	t.Helper()
	conn, err := Open(&Options{
		Addr:         []string{addr},
		Protocol:     HTTP,
		MaxOpenConns: 1,
		MaxIdleConns: 1,
		DialTimeout:  5 * time.Second,
		Settings:     settings,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// driverConn is the concrete conn returned by Open. Tests use the public API.
type driverConn interface {
	QueryFormat(ctx context.Context, format string, query string, args ...any) (io.ReadCloser, error)
	ServerVersion() (*proto.ServerHandshake, error)
	Close() error
}

func TestQueryHelloIgnoresPerQueryOptions(t *testing.T) {
	srv, got := serveHTTPHello(t)
	conn := openHTTP(t, srv.Listener.Addr().String(), Settings{
		"session_timezone": "Europe/Berlin",
		"max_threads":      2,
	})

	extTable, err := ext.NewTable("ext", ext.Column("a", column.Type("String")))
	require.NoError(t, err)
	ctx := Context(context.Background(),
		WithSettings(Settings{
			"session_timezone":                                   "Asia/Tokyo",
			"client_protocol_version":                            0,
			"output_format_native_encode_types_in_binary_format": 1,
		}),
		WithQueryID("caller-query"),
		WithQuotaKey("caller-quota"),
		WithParameters(Parameters{"p": "v"}),
		WithExternalTable(extTable),
	)

	stream, err := conn.QueryFormat(ctx, "CSV", "SELECT 1")
	require.NoError(t, err)
	payload, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Equal(t, "1\n", string(payload))

	// A later call on the pooled connection, with no per-query options, must
	// see the timezone recorded from the handshake, not from the first call.
	ver, err := conn.ServerVersion()
	require.NoError(t, err)
	require.Equal(t, "Europe/Berlin", ver.Timezone.String())

	plain, err := conn.QueryFormat(context.Background(), "CSV", "SELECT 2")
	require.NoError(t, err)
	require.NoError(t, plain.Close())

	hello, user := splitHello(*got)
	require.Len(t, hello, 1, "the pooled connection handshakes once")
	require.GreaterOrEqual(t, len(user), 1)

	h := hello[0].query
	require.Equal(t, "Europe/Berlin", h.Get("session_timezone"),
		"connection-level session_timezone stays on the handshake")
	require.Equal(t, "2", h.Get("max_threads"))
	require.Equal(t, strconv.Itoa(ClientTCPProtocolVersion), h.Get("client_protocol_version"))
	require.NotContains(t, h, "output_format_native_encode_types_in_binary_format")
	require.Empty(t, h.Get("query_id"))
	require.Empty(t, h.Get("quota_key"))
	require.Empty(t, h.Get("param_p"))
	require.Empty(t, h.Get("ext_format"), "external tables are not part of the handshake")
	require.Contains(t, hello[0].body, helloQuery)

	first := user[0].query
	require.Equal(t, "Asia/Tokyo", first.Get("session_timezone"))
	require.Equal(t, "0", first.Get("client_protocol_version"))
	require.Equal(t, "1", first.Get("output_format_native_encode_types_in_binary_format"))
	require.Equal(t, "caller-query", first.Get("query_id"))
	require.Equal(t, "caller-quota", first.Get("quota_key"))
	require.Equal(t, "v", first.Get("param_p"))
	require.Equal(t, "CSV", first.Get("default_format"))
	require.Contains(t, user[0].body, "SELECT 1")

	// The follow-up call has no per-query options, so it must not repeat the
	// first call's session_timezone.
	var follow url.Values
	for _, c := range user {
		if strings.Contains(c.body, "SELECT 2") {
			follow = c.query
		}
	}
	require.NotNil(t, follow)
	require.Equal(t, "Europe/Berlin", follow.Get("session_timezone"))
	require.Empty(t, follow.Get("query_id"))
}

func TestQueryHelloNoConnSettingsUsesServerTimezone(t *testing.T) {
	srv, got := serveHTTPHello(t)
	conn := openHTTP(t, srv.Listener.Addr().String(), nil)

	ctx := Context(context.Background(),
		WithSettings(Settings{"session_timezone": "Asia/Tokyo"}))
	stream, err := conn.QueryFormat(ctx, "CSV", "SELECT 1")
	require.NoError(t, err)
	require.NoError(t, stream.Close())

	ver, err := conn.ServerVersion()
	require.NoError(t, err)
	require.Equal(t, "UTC", ver.Timezone.String())

	hello, _ := splitHello(*got)
	require.Len(t, hello, 1)
	require.Empty(t, hello[0].query.Get("session_timezone"))
}

func TestContextWithoutQueryOptionsDropsCallerOptions(t *testing.T) {
	extTable, err := ext.NewTable("ext", ext.Column("a", column.Type("String")))
	require.NoError(t, err)
	parent, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	parent = Context(parent,
		WithSettings(Settings{"session_timezone": "Asia/Tokyo"}),
		WithQueryID("caller-query"),
		WithQuotaKey("caller-quota"),
		WithParameters(Parameters{"p": "v"}),
		WithExternalTable(extTable),
		WithClientInfo(ClientInfo{Comment: []string{"caller"}}),
	)

	stripped := contextWithoutQueryOptions(parent)
	opts := queryOptions(stripped)
	require.NotContains(t, opts.settings, "session_timezone")
	require.Contains(t, opts.settings, "max_execution_time",
		"the dial deadline still bounds the handshake; that is independent of per-query settings")
	require.Empty(t, opts.queryID)
	require.Empty(t, opts.quotaKey)
	require.Empty(t, opts.parameters)
	require.Empty(t, opts.external)
	require.Empty(t, opts.clientInfo.Comment)

	cancel()
	require.Error(t, stripped.Err())
}

func splitHello(got []capturedHTTPQuery) (hello, rest []capturedHTTPQuery) {
	for _, c := range got {
		if strings.Contains(c.body, "displayName()") {
			hello = append(hello, c)
			continue
		}
		rest = append(rest, c)
	}
	return hello, rest
}
