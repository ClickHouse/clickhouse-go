package tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Finding 1 (end-to-end): a successful QueryFormat whose result data contains
// the "__exception__" bytes must be returned verbatim, not truncated into a
// fabricated error.
func TestFormatMarkerInResultDataVerbatim(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()
	ctx := context.Background()

	cases := map[string]string{
		"bare marker":            "before__exception__after",
		"marker with fake code":  "__exception__ Code: 373. DB::Exception: fake. (SESSION_IS_LOCKED)",
		"marker then whitespace": "x__exception__ y",
	}
	for name, val := range cases {
		t.Run(name, func(t *testing.T) {
			stream, err := conn.QueryFormat(ctx, "TabSeparatedRaw",
				fmt.Sprintf("SELECT %s", quote(val)))
			require.NoError(t, err)
			defer stream.Close()
			got, err := io.ReadAll(stream)
			require.NoError(t, err, "valid data containing the marker must not fabricate an error")
			assert.Equal(t, val+"\n", string(got))
		})
	}
}

// Finding 1 (positive side): a genuine mid-stream exception during a format
// query is still surfaced as an error.
func TestFormatGenuineMidStreamException(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_threads":                           1,
		"max_block_size":                        1,
		"wait_end_of_query":                     0,
		"http_response_buffer_size":             1,
		"http_write_exception_in_output_format": 0,
	}))

	stream, err := conn.QueryFormat(ctx, "CSV", "SELECT throwIf(number=3, 'boom mid stream') FROM system.numbers")
	require.NoError(t, err)
	defer stream.Close()
	_, err = io.ReadAll(stream)
	require.Error(t, err, "a mid-stream server exception must surface as an error")
	assert.Contains(t, err.Error(), "boom mid stream")
}

// Finding 2 (end-to-end): a real compressed insert round-trip still works.
func TestFormatCompressedInsertRoundTrip(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil,
		&clickhouse.Compression{Method: clickhouse.CompressionGZIP})
	require.NoError(t, err)
	defer conn.Close()
	ctx := context.Background()

	source := createFormatTestTable(t, conn, true)
	dest := createFormatTestTable(t, conn, false)

	stream, err := conn.QueryFormat(ctx, "Parquet",
		fmt.Sprintf("SELECT id, name, score, ok, created_at, comment FROM %s ORDER BY id", source))
	require.NoError(t, err)
	payload, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.NotEmpty(t, payload)

	require.NoError(t, conn.InsertFormat(ctx, "Parquet",
		fmt.Sprintf("INSERT INTO %s", dest), bytes.NewReader(payload)))
	verifyFormatTestTable(t, conn, dest)
}

func quote(s string) string {
	return "'" + string(bytes.ReplaceAll([]byte(s), []byte("'"), []byte("\\'"))) + "'"
}
