package column

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestArrayColumn(tb testing.TB, chType Type) *Array {
	tb.Helper()
	col, err := chType.Column("test", nil)
	require.NoError(tb, err)
	arr, ok := col.(*Array)
	require.True(tb, ok)
	return arr
}

func testStringRows(rows, elems int) [][]string {
	out := make([][]string, rows)
	for i := range out {
		row := make([]string, elems)
		for j := range row {
			row[j] = fmt.Sprintf("value-%d-%d", i, j)
		}
		out[i] = row
	}
	out[0] = nil
	return out
}

func TestArrayStringAppend(t *testing.T) {
	rows := testStringRows(5, 3)
	for _, chType := range []Type{"Array(String)", "Array(Nullable(String))"} {
		t.Run(string(chType)+"/AppendRow", func(t *testing.T) {
			col := newTestArrayColumn(t, chType)
			for _, row := range rows {
				require.NoError(t, col.AppendRow(row))
			}
			assertArrayStringRows(t, col, rows)
		})
		t.Run(string(chType)+"/Append", func(t *testing.T) {
			col := newTestArrayColumn(t, chType)
			_, err := col.Append(rows)
			require.NoError(t, err)
			assertArrayStringRows(t, col, rows)
		})
	}
}

func assertArrayStringRows(t *testing.T, col *Array, rows [][]string) {
	t.Helper()
	require.Equal(t, len(rows), col.Rows())
	for i, want := range rows {
		var got []string
		if _, nullable := col.values.(*Nullable); nullable {
			var ptrs []*string
			require.NoError(t, col.ScanRow(&ptrs, i))
			for _, p := range ptrs {
				got = append(got, *p)
			}
		} else {
			require.NoError(t, col.ScanRow(&got, i))
		}
		if len(want) == 0 {
			assert.Empty(t, got, "row %d", i)
			continue
		}
		assert.Equal(t, want, got, "row %d", i)
	}
}

func BenchmarkArrayStringAppendRow(b *testing.B) {
	rows := testStringRows(1024, 12)
	col := newTestArrayColumn(b, "Array(String)")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := col.AppendRow(rows[i%len(rows)]); err != nil {
			b.Fatal(err)
		}
		if (i+1)%len(rows) == 0 {
			col.Reset()
		}
	}
}

func BenchmarkArrayStringAppend(b *testing.B) {
	rows := testStringRows(1024, 12)
	col := newTestArrayColumn(b, "Array(String)")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.Append(rows); err != nil {
			b.Fatal(err)
		}
		col.Reset()
	}
}
