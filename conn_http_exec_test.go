package clickhouse

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPExecEmptyBodyIsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := newTestHTTPConnect(t, srv.URL)
	require.NoError(t, h.exec(context.Background(), "CREATE TABLE t (a Int64) ENGINE = Memory"))
}

func TestHTTPExecInBandExceptionOn200(t *testing.T) {
	// Same hole insertFormat already closed: a DDL/exec whose failure arrives
	// after the 200 headers must not report success.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(taggedExceptionPayload))
	}))
	defer srv.Close()

	h := newTestHTTPConnect(t, srv.URL)
	err := h.exec(context.Background(), "DROP TABLE t")
	require.Error(t, err)
	var ex *Exception
	require.ErrorAs(t, err, &ex)
	assert.Equal(t, int32(395), ex.Code)
}

func TestHTTPExecExceptionCodeHeaderOn200(t *testing.T) {
	// Mid-stream failure after the status line was flushed: some servers keep
	// HTTP 200 and put the code on X-ClickHouse-Exception-Code with a plain
	// (not "__exception__"-framed) body. Exec used to discard that as success.
	const body = "Code: 44. DB::Exception: Sorting key contains nullable columns. (ILLEGAL_COLUMN)"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set(exceptionCodeHeader, "44")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	h := newTestHTTPConnect(t, srv.URL)
	err := h.exec(context.Background(),
		"CREATE TABLE t (a Nullable(String)) ENGINE = MergeTree PRIMARY KEY a")
	require.Error(t, err)
	var ex *Exception
	require.ErrorAs(t, err, &ex)
	assert.Equal(t, int32(44), ex.Code)
	assert.Contains(t, err.Error(), "nullable")
}

func TestInsertFormatExceptionCodeHeaderOn200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set(exceptionCodeHeader, "44")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Code: 44. DB::Exception: Sorting key contains nullable columns"))
	}))
	defer srv.Close()

	h := newTestHTTPConnect(t, srv.URL)
	err := h.insertFormat(context.Background(),
		func(_ nativeTransport, _ error) {},
		"CSV", "INSERT INTO t (a)", strings.NewReader("1\n"))
	require.Error(t, err)
	var ex *Exception
	require.ErrorAs(t, err, &ex)
	assert.Equal(t, int32(44), ex.Code)
}
