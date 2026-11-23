package column

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// encodeDecodeColumn encodes col and decodes the result into a new column of the same type.
func encodeDecodeColumn(t *testing.T, col Interface) Interface {
	t.Helper()
	var buf chproto.Buffer
	col.Encode(&buf)
	decoded, err := col.Type().Column(col.Name(), &ServerContext{})
	require.NoError(t, err)
	require.NoError(t, decoded.Decode(chproto.NewReader(bytes.NewReader(buf.Buf)), col.Rows()))
	return decoded
}

// appendEncodeDecode appends values to a new column of type chType, encodes it and
// decodes the result into a new column.
func appendEncodeDecode(t *testing.T, chType Type, values []any) Interface {
	t.Helper()
	col, err := chType.Column("test", nil)
	require.NoError(t, err)
	for _, v := range values {
		require.NoError(t, col.AppendRow(v), "append %#v", v)
	}
	return encodeDecodeColumn(t, col)
}

// rowValue returns row i of col. A pointer is replaced by the value it points to, so that
// a failed assertion prints the value.
func rowValue(col Interface, i int) any {
	switch v := col.Row(i, false).(type) {
	case *string:
		return *v
	default:
		return v
	}
}

type errorValuer struct{}

func (errorValuer) Value() (driver.Value, error) {
	return nil, errors.New("value error")
}

// TestLowCardinalityAppendRowValuerNull verifies that a driver.Valuer whose Value method
// returns nil is NULL in LowCardinality(Nullable(String)), as it is in Nullable(String).
// In LowCardinality(String) it is an empty string, as it is in String.
func TestLowCardinalityAppendRowValuerNull(t *testing.T) {
	values := []any{
		nil,
		(*string)(nil),
		sql.NullString{},
		&sql.NullString{},
		sql.Null[string]{},
		&sql.Null[string]{},
		sql.NullString{String: "a", Valid: true},
		sql.Null[string]{V: "b", Valid: true},
		"c",
	}
	testCases := []struct {
		lowCardinality Type
		plain          Type
		expected       []any
	}{
		{
			lowCardinality: "LowCardinality(Nullable(String))",
			plain:          "Nullable(String)",
			expected:       []any{nil, nil, nil, nil, nil, nil, "a", "b", "c"},
		},
		{
			lowCardinality: "LowCardinality(String)",
			plain:          "String",
			expected:       []any{"", "", "", "", "", "", "a", "b", "c"},
		},
	}
	for _, tc := range testCases {
		t.Run(string(tc.lowCardinality), func(t *testing.T) {
			lc := appendEncodeDecode(t, tc.lowCardinality, values)
			plain := appendEncodeDecode(t, tc.plain, values)
			for i, v := range values {
				assert.Equal(t, tc.expected[i], rowValue(lc, i), "%s: row %d: %#v", tc.lowCardinality, i, v)
				assert.Equal(t, rowValue(plain, i), rowValue(lc, i), "%s and %s: row %d: %#v", tc.lowCardinality, tc.plain, i, v)
			}
		})
	}
}

// TestLowCardinalityAppendRowValuerError verifies that AppendRow returns the error of a
// driver.Valuer.
func TestLowCardinalityAppendRowValuerError(t *testing.T) {
	for _, chType := range []Type{"LowCardinality(Nullable(String))", "LowCardinality(String)"} {
		col, err := chType.Column("test", nil)
		require.NoError(t, err)
		assert.Error(t, col.AppendRow(errorValuer{}), chType)
	}
}

// TestLowCardinalityAppendRowReusedPointer verifies that AppendRow uses the value that
// a pointer points to as the dictionary key, not the pointer address. A caller can
// reuse one pointer for rows with different values.
func TestLowCardinalityAppendRowReusedPointer(t *testing.T) {
	for _, chType := range []Type{"LowCardinality(String)", "LowCardinality(Nullable(String))"} {
		t.Run(string(chType), func(t *testing.T) {
			col, err := chType.Column("test", nil)
			require.NoError(t, err)
			var v string
			for _, name := range []string{"apple", "banana", "apple", "cherry"} {
				v = name
				require.NoError(t, col.AppendRow(&v))
			}
			// A value and a pointer to the same value use one dictionary entry.
			require.NoError(t, col.AppendRow("banana"))
			assert.Len(t, col.(*LowCardinality).append.index, 3)

			decoded := encodeDecodeColumn(t, col)
			for i, expected := range []string{"apple", "banana", "apple", "cherry", "banana"} {
				assert.Equal(t, expected, rowValue(decoded, i), "%s: row %d", chType, i)
			}
		})
	}

	t.Run("*sql.NullString", func(t *testing.T) {
		col, err := Type("LowCardinality(Nullable(String))").Column("test", nil)
		require.NoError(t, err)
		var v sql.NullString
		for _, value := range []sql.NullString{{String: "a", Valid: true}, {}, {String: "b", Valid: true}} {
			v = value
			require.NoError(t, col.AppendRow(&v))
		}

		decoded := encodeDecodeColumn(t, col)
		for i, expected := range []any{"a", nil, "b"} {
			assert.Equal(t, expected, rowValue(decoded, i), "row %d", i)
		}
	})

	t.Run("*time.Time", func(t *testing.T) {
		col, err := Type("LowCardinality(DateTime('UTC'))").Column("test", &ServerContext{})
		require.NoError(t, err)
		start := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
		offsets := []time.Duration{0, time.Second, 500 * time.Millisecond, 2 * time.Second}
		var v time.Time
		for _, offset := range offsets {
			v = start.Add(offset)
			require.NoError(t, col.AppendRow(&v))
		}
		// The truncation to seconds also applies to a pointer, so the values at
		// 0s and at 0.5s use one dictionary entry.
		assert.Len(t, col.(*LowCardinality).append.index, 3)

		decoded := encodeDecodeColumn(t, col)
		for i, offset := range offsets {
			expected := start.Add(offset).Truncate(time.Second)
			assert.Equal(t, expected.Unix(), decoded.Row(i, false).(time.Time).Unix(), "row %d", i)
		}
	})

	t.Run("*[]byte", func(t *testing.T) {
		// A slice is not comparable, so the pointer address stays the key.
		col, err := Type("LowCardinality(String)").Column("test", nil)
		require.NoError(t, err)
		a, b := []byte("a"), []byte("b")
		for _, v := range []*[]byte{&a, &b, &a} {
			require.NoError(t, col.AppendRow(v))
		}

		decoded := encodeDecodeColumn(t, col)
		for i, expected := range []string{"a", "b", "a"} {
			assert.Equal(t, expected, rowValue(decoded, i), "row %d", i)
		}
	})
}
