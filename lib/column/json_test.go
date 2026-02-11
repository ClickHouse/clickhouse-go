package column

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestJSONColumn creates a JSON column with unset serialization version (default state)
func newTestJSONColumn(t *testing.T) *JSON {
	t.Helper()
	sc := &ServerContext{}
	col, err := (&JSON{name: "test"}).parse("JSON", sc)
	require.NoError(t, err)
	return col
}

func TestJSONAppendRowNilConsistency(t *testing.T) {
	type testStruct struct {
		Name string `json:"name"`
	}

	tests := []struct {
		name            string
		rows            []any
		wantErr         bool
		expectedVersion uint64
		expectedRows    int
	}{
		{
			name:            "nil then struct - should use object version",
			rows:            []any{nil, testStruct{Name: "Alice"}},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "struct then nil - should use object version",
			rows:            []any{testStruct{Name: "Alice"}, nil},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "nil then map - should use object version",
			rows:            []any{nil, map[string]any{"key": "value"}},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "map then nil - should use object version",
			rows:            []any{map[string]any{"key": "value"}, nil},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "nil then *chcol.JSON - should use object version",
			rows:            []any{nil, chcol.NewJSON()},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "*chcol.JSON then nil - should use object version",
			rows:            []any{chcol.NewJSON(), nil},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "nil then struct pointer - should use object version",
			rows:            []any{nil, &testStruct{Name: "Bob"}},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "nil only - should use object version",
			rows:            []any{nil},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    1,
		},
		{
			name:            "multiple nils then struct - should use object version",
			rows:            []any{nil, nil, testStruct{Name: "Alice"}},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    3,
		},
		{
			name:            "nil between structs - should use object version",
			rows:            []any{testStruct{Name: "Alice"}, nil, testStruct{Name: "Bob"}},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    3,
		},
		{
			name: "nil then pointer to nil interface - should use object version",
			rows: func() []any {
				var s any
				return []any{s, &s}
			}(),
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name: "pointer to nil interface then nil - should use object version",
			rows: func() []any {
				var s any
				return []any{&s, s}
			}(),
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newTestJSONColumn(t)

			var lastErr error
			for _, row := range tt.rows {
				if err := col.AppendRow(row); err != nil {
					lastErr = err
					break
				}
			}

			if tt.wantErr {
				require.Error(t, lastErr)
			} else {
				require.NoError(t, lastErr)
				assert.Equal(t, tt.expectedRows, col.Rows())
			}

			assert.Equal(t, tt.expectedVersion, col.serializationVersion)
		})
	}
}
