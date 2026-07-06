package format

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type testColumn struct {
	name string
	typ  column.Type
}

var testColumns = []testColumn{
	{"id", "Int64"},
	{"flag", "UInt8"},
	{"score", "Float64"},
	{"name", "String"},
	{"ok", "Bool"},
	{"created_at", "DateTime('UTC')"},
	{"birthday", "Date"},
	{"comment", "Nullable(String)"},
	{"rank", "Nullable(Int32)"},
}

var testRows = [][]any{
	{int64(1), uint8(200), 3.14, "alice", true, time.Date(2026, 7, 6, 10, 30, 0, 0, time.UTC), time.Date(1990, 1, 2, 0, 0, 0, 0, time.UTC), "hello, \"world\"", int32(42)},
	{int64(-2), uint8(0), -0.5, "bob\nnewline", false, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC), nil, nil},
	{int64(3), uint8(7), 1e10, "", true, time.Date(2026, 7, 6, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), `\N looks like null`, int32(-1)},
}

func newTestBlock(t *testing.T, populate bool) *proto.Block {
	t.Helper()
	block := proto.NewBlock()
	block.ServerContext.Timezone = time.UTC
	for _, c := range testColumns {
		require.NoError(t, block.AddColumn(c.name, c.typ))
	}
	if populate {
		for _, row := range testRows {
			require.NoError(t, block.Append(row...))
		}
	}
	return block
}

func assertBlocksEqual(t *testing.T, want, got *proto.Block) {
	t.Helper()
	require.Equal(t, want.Rows(), got.Rows())
	for i, wantCol := range want.Columns {
		gotCol := got.Columns[i]
		for row := 0; row < want.Rows(); row++ {
			wantVal, gotVal := wantCol.Row(row, false), gotCol.Row(row, false)
			if wantTime, ok := wantVal.(time.Time); ok {
				gotTime, ok := gotVal.(time.Time)
				require.True(t, ok, "column %s row %d: expected time.Time, got %T", wantCol.Name(), row, gotVal)
				assert.True(t, wantTime.Equal(gotTime), "column %s row %d: %v != %v", wantCol.Name(), row, wantTime, gotTime)
				continue
			}
			assert.Equal(t, wantVal, gotVal, "column %s row %d", wantCol.Name(), row)
		}
	}
}

func testRoundTrip(t *testing.T, codec Codec) {
	source := newTestBlock(t, true)

	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf)
	require.NoError(t, enc.WriteBlock(newTestBlock(t, false)), "schema-only block must be accepted")
	require.NoError(t, enc.WriteBlock(source))
	require.NoError(t, enc.Close())

	dest := newTestBlock(t, false)
	dec := codec.NewDecoder(&buf)
	n, err := dec.ReadBlock(dest, 1000)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, len(testRows), n)
	assertBlocksEqual(t, source, dest)
}

func TestCSVRoundTrip(t *testing.T) {
	testRoundTrip(t, CSV{})
}

func TestJSONEachRowRoundTrip(t *testing.T) {
	testRoundTrip(t, JSONEachRow{})
}

func TestParquetRoundTrip(t *testing.T) {
	testRoundTrip(t, Parquet{})
}

func TestArrowStreamRoundTrip(t *testing.T) {
	testRoundTrip(t, ArrowStream{})
}

func TestCSVNull(t *testing.T) {
	block := newTestBlock(t, true)
	var buf bytes.Buffer
	enc := CSV{}.NewEncoder(&buf)
	require.NoError(t, enc.WriteBlock(block))
	require.NoError(t, enc.Close())
	assert.Contains(t, buf.String(), `\N`)
}

func TestDecoderMaxRows(t *testing.T) {
	for _, codec := range []Codec{CSV{}, JSONEachRow{}, Parquet{}, ArrowStream{}} {
		t.Run(codec.Name(), func(t *testing.T) {
			source := newTestBlock(t, true)
			var buf bytes.Buffer
			enc := codec.NewEncoder(&buf)
			require.NoError(t, enc.WriteBlock(source))
			require.NoError(t, enc.Close())

			dest := newTestBlock(t, false)
			dec := codec.NewDecoder(&buf)
			n, err := dec.ReadBlock(dest, 2)
			require.NoError(t, err)
			require.Equal(t, 2, n)

			dest.Reset()
			n, err = dec.ReadBlock(dest, 2)
			require.ErrorIs(t, err, io.EOF)
			require.Equal(t, 1, n)
		})
	}
}

func TestCSVDecodeError(t *testing.T) {
	dest := newTestBlock(t, false)
	dec := CSV{}.NewDecoder(strings.NewReader("not-a-number,1,1.0,x,true,2026-01-01 00:00:00,2026-01-01,\\N,\\N\n"))
	_, err := dec.ReadBlock(dest, 10)
	require.Error(t, err)
	require.False(t, errors.Is(err, io.EOF))
	assert.Contains(t, err.Error(), "row 1")
	assert.Contains(t, err.Error(), "id")
}

func TestJSONEachRowMissingKeyAppendsDefault(t *testing.T) {
	dest := newTestBlock(t, false)
	dec := JSONEachRow{}.NewDecoder(strings.NewReader(`{"id":7}` + "\n"))
	n, err := dec.ReadBlock(dest, 10)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, n)
	assert.Equal(t, int64(7), dest.Columns[0].Row(0, false))
	assert.Equal(t, "", dest.Columns[3].Row(0, false))
	assert.Nil(t, dest.Columns[7].Row(0, false))
}
