package column

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppendNilPointer verifies that a nil pointer is appended as an untyped nil is:
// the zero value in T and NULL in Nullable(T).
func TestAppendNilPointer(t *testing.T) {
	tests := []struct {
		chType Type
		value  any
		slice  any
	}{
		{"String", (*[]byte)(nil), nil},
		{"String", (*json.RawMessage)(nil), []*json.RawMessage{nil}},
		{"String", (*sql.NullString)(nil), []*sql.NullString{nil}},
		{"Int16", (*sql.NullInt16)(nil), []*sql.NullInt16{nil}},
		{"Int32", (*sql.NullInt32)(nil), []*sql.NullInt32{nil}},
		{"Int64", (*sql.NullInt64)(nil), []*sql.NullInt64{nil}},
		{"Int64", (*time.Duration)(nil), nil},
		{"Float64", (*sql.NullFloat64)(nil), []*sql.NullFloat64{nil}},
		{"Date", (*sql.NullTime)(nil), []*sql.NullTime{nil}},
		{"Date32", (*sql.NullTime)(nil), []*sql.NullTime{nil}},
		{"DateTime", (*sql.NullTime)(nil), []*sql.NullTime{nil}},
		{"DateTime64(3)", (*sql.NullTime)(nil), []*sql.NullTime{nil}},
	}
	for _, tt := range tests {
		for _, chType := range []Type{tt.chType, "Nullable(" + tt.chType + ")"} {
			newColumn := func(t *testing.T) Interface {
				col, err := chType.Column("x", &ServerContext{})
				require.NoError(t, err)
				return col
			}
			expected := newColumn(t)
			require.NoError(t, expected.AppendRow(nil))
			want := expected.Row(0, false)

			t.Run(string(chType)+"/AppendRow/"+typeName(tt.value), func(t *testing.T) {
				col := newColumn(t)
				require.NoError(t, col.AppendRow(tt.value))
				require.Equal(t, 1, col.Rows())
				assert.Equal(t, want, col.Row(0, false))
			})
			if tt.slice == nil {
				continue
			}
			t.Run(string(chType)+"/Append/"+typeName(tt.slice), func(t *testing.T) {
				col := newColumn(t)
				nulls, err := col.Append(tt.slice)
				require.NoError(t, err)
				assert.Equal(t, []uint8{1}, nulls)
				require.Equal(t, 1, col.Rows())
				assert.Equal(t, want, col.Row(0, false))
			})
		}
	}
}

func typeName(v any) string {
	return reflect.TypeOf(v).String()
}
