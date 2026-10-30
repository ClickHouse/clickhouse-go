package column

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
)

func TestDynamicInferredTypes(t *testing.T) {
	for _, sc := range []ServerContext{{VersionMajor: 25, VersionMinor: 5}, {VersionMajor: 26, VersionMinor: 9}} {
		t.Run(fmt.Sprintf("%d.%d", sc.VersionMajor, sc.VersionMinor), func(t *testing.T) {
			values := []any{[]string{"x", "y"}, "z", []int64{1, 2}, "", int32(42), int64(1 << 40), nil}
			want := []chcol.Dynamic{
				chcol.NewDynamicWithType([]string{"x", "y"}, "Array(String)"),
				chcol.NewDynamicWithType("z", "String"),
				chcol.NewDynamicWithType([]int64{1, 2}, "Array(Int64)"),
				chcol.NewDynamicWithType("", "String"),
				chcol.NewDynamicWithType(int32(42), "Int32"),
				chcol.NewDynamicWithType(int64(1<<40), "Int64"),
				chcol.NewDynamicWithType(nil, ""),
			}
			require.Equal(t, want, roundTripDynamic(t, &sc, values))
		})
	}
}

func TestDynamicExplicitAndFallbackTypes(t *testing.T) {
	sc := &ServerContext{VersionMajor: 26, VersionMinor: 9}
	values := []any{chcol.NewDynamicWithType(uint64(7), "Int64"), time.Duration(8)}
	want := []chcol.Dynamic{chcol.NewDynamicWithType(int64(7), "Int64"), chcol.NewDynamicWithType(int64(8), "Int64")}
	require.Equal(t, want, roundTripDynamic(t, sc, values))
}

func roundTripDynamic(t *testing.T, sc *ServerContext, values []any) []chcol.Dynamic {
	t.Helper()
	col, err := (&Dynamic{name: "value"}).parse("Dynamic", sc)
	require.NoError(t, err)
	nonNullRows := 0
	for _, value := range values {
		require.NoError(t, col.AppendRow(value))
		if value != nil {
			nonNullRows++
		}
	}
	// A failed type probe must not leave extra rows in a candidate column.
	storedRows := 0
	for _, nested := range col.columns {
		storedRows += nested.Rows()
	}
	require.Equal(t, nonNullRows, storedRows)

	buf := &proto.Buffer{}
	require.NoError(t, col.WriteStatePrefix(buf))
	col.Encode(buf)
	decoded, err := (&Dynamic{name: "value"}).parse("Dynamic", sc)
	require.NoError(t, err)
	reader := proto.NewReader(bytes.NewReader(buf.Buf))
	require.NoError(t, decoded.ReadStatePrefix(reader))
	require.NoError(t, decoded.Decode(reader, len(values)))
	_, err = reader.ReadByte()
	require.ErrorIs(t, err, io.EOF)

	got := make([]chcol.Dynamic, len(values))
	for i := range got {
		require.NoError(t, decoded.ScanRow(&got[i], i))
	}
	return got
}
