package column

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
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

func TestDynamicFallbackArrayTypes(t *testing.T) {
	for _, sc := range []ServerContext{{VersionMajor: 25, VersionMinor: 5}, {VersionMajor: 26, VersionMinor: 9}} {
		t.Run(fmt.Sprintf("%d.%d", sc.VersionMajor, sc.VersionMinor), func(t *testing.T) {
			values := []any{[]string{"x"}, []int64{1}, []any{int64(2)}, []int{3}, reflect.ValueOf([]int{4})}
			want := []chcol.Dynamic{
				chcol.NewDynamicWithType([]string{"x"}, "Array(String)"),
				chcol.NewDynamicWithType([]int64{1}, "Array(Int64)"),
				chcol.NewDynamicWithType([]int64{2}, "Array(Int64)"),
				chcol.NewDynamicWithType([]int64{3}, "Array(Int64)"),
				chcol.NewDynamicWithType([]int64{4}, "Array(Int64)"),
			}
			require.Equal(t, want, roundTripDynamic(t, &sc, values))
		})
	}
}

func TestDynamicExplicitTypesRemainPreferred(t *testing.T) {
	timestamp := time.Date(2026, 11, 10, 12, 34, 56, 123456789, time.UTC)
	for _, sc := range []ServerContext{{VersionMajor: 25, VersionMinor: 5, Timezone: time.UTC}, {VersionMajor: 26, VersionMinor: 9, Timezone: time.UTC}} {
		t.Run(fmt.Sprintf("%d.%d", sc.VersionMajor, sc.VersionMinor), func(t *testing.T) {
			for _, test := range []struct {
				name                string
				chType              string
				first, second, want any
			}{
				{"datetime_precision", "DateTime64(9)", timestamp, timestamp.Add(time.Nanosecond), timestamp.Add(time.Nanosecond)},
				{"low_cardinality_string", "LowCardinality(String)", "first", "second", "second"},
				{"float_conversion", "Float64", float64(1.25), float32(2.5), float64(2.5)},
				{"array_low_cardinality", "Array(LowCardinality(String))", []string{"first"}, []string{"second"}, []string{"second"}},
				{"map_value", "Map(String, Int64)", map[string]int64{"first": 1}, map[string]int64{"second": 2}, map[string]int64{"second": 2}},
				{"tuple_conversion", "Tuple(String, Array(Int64))", []any{"first", []int64{1}}, []any{"second", []int{2}}, []any{"second", []int64{2}}},
			} {
				t.Run(test.name, func(t *testing.T) {
					values := []any{chcol.NewDynamicWithType(test.first, test.chType), test.second}
					want := []chcol.Dynamic{chcol.NewDynamicWithType(test.first, test.chType), chcol.NewDynamicWithType(test.want, test.chType)}
					require.Equal(t, want, roundTripDynamic(t, &sc, values))
				})
			}
		})
	}
}

func TestDynamicRejectedFallbackLeavesColumnsIntact(t *testing.T) {
	type namedString string
	sc := &ServerContext{VersionMajor: 26, VersionMinor: 9}
	col, err := (&Dynamic{name: "value"}).parse("Dynamic", sc)
	require.NoError(t, err)
	require.NoError(t, col.AppendRow([]string{"original"}))
	for _, value := range []any{[]any{"partial", int64(2)}, namedString("")} {
		require.Error(t, col.AppendRow(value))
		require.Equal(t, 1, col.Rows())
		require.Equal(t, 1, col.columns[0].Rows())
		array := col.columns[0].(*Array)
		require.Equal(t, 1, array.values.Rows())
		require.Equal(t, []string{"original"}, array.Row(0, false))
	}
}

type dynamicTestValuer func() (driver.Value, error)

func (f dynamicTestValuer) Value() (driver.Value, error) { return f() }

func TestDynamicFallbackValuer(t *testing.T) {
	calls := 0
	valuer := dynamicTestValuer(func() (driver.Value, error) {
		calls++
		return int64(2), nil
	})
	values := []any{[]string{"x"}, int64(1), valuer}
	want := []chcol.Dynamic{
		chcol.NewDynamicWithType([]string{"x"}, "Array(String)"),
		chcol.NewDynamicWithType(int64(1), "Int64"),
		chcol.NewDynamicWithType(int64(2), "Int64"),
	}
	require.Equal(t, want, roundTripDynamic(t, &ServerContext{VersionMajor: 26, VersionMinor: 9}, values))
	require.Equal(t, 1, calls)
}

func TestDynamicExplicitTypeAfterReset(t *testing.T) {
	timestamp := time.Date(2026, 11, 10, 12, 34, 56, 123456789, time.UTC)
	col, err := (&Dynamic{name: "value"}).parse("Dynamic", &ServerContext{VersionMajor: 26, VersionMinor: 9, Timezone: time.UTC})
	require.NoError(t, err)
	require.NoError(t, col.AppendRow(chcol.NewDynamicWithType(timestamp, "DateTime64(9)")))
	col.Reset()
	require.NoError(t, col.AppendRow(timestamp.Add(time.Nanosecond)))
	require.Equal(t, 1, col.Rows())
	require.Len(t, col.columns, 1)
	require.Equal(t, timestamp.Add(time.Nanosecond), col.columns[0].Row(0, false))
}

func TestDynamicConversionsAcrossColumns(t *testing.T) {
	values := []any{
		chcol.NewDynamicWithType("first", "LowCardinality(String)"), "second",
		chcol.NewDynamicWithType(float64(1.25), "Float64"), float32(2.5), "third", float32(3.5),
	}
	want := []chcol.Dynamic{
		chcol.NewDynamicWithType("first", "LowCardinality(String)"),
		chcol.NewDynamicWithType("second", "LowCardinality(String)"),
		chcol.NewDynamicWithType(float64(1.25), "Float64"),
		chcol.NewDynamicWithType(float64(2.5), "Float64"),
		chcol.NewDynamicWithType("third", "LowCardinality(String)"),
		chcol.NewDynamicWithType(float64(3.5), "Float64"),
	}
	require.Equal(t, want, roundTripDynamic(t, &ServerContext{VersionMajor: 26, VersionMinor: 9}, values))
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
