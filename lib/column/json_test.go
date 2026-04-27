package column

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
)

// newTestJSONColumn creates a JSON column with unset serialization version (default state)
func newTestJSONColumn(t *testing.T) *JSON {
	t.Helper()
	sc := &ServerContext{}
	col, err := (&JSON{name: "test"}).parse("JSON", sc)
	require.NoError(t, err)
	return col
}

// TestJSONAppendRowNilConsistency verifies the contract for AppendRow(nil).
// Key rules:
//  1. A nil row carries no mode preference — it does NOT latch the column.
//     Instead it bumps pendingNullRows and c.rows.
//  2. The first non-nil row latches the mode via reconcileMode, and flushes
//     any pending nulls into the latched backing column at that moment.
//  3. An all-null batch leaves the column Unset after AppendRow; it will be
//     latched to String at WriteStatePrefix time and flushed with "null"
//     payloads.
func TestJSONAppendRowNilConsistency(t *testing.T) {
	type testStruct struct {
		Name string `json:"name"`
	}

	// jsonUnset is math.MaxUint64; the parse() helper initializes every
	// fresh JSON column to this sentinel.
	const jsonUnset = JSONUnsetSerializationVersion

	tests := []struct {
		name                string
		rows                []any
		wantErr             bool
		expectedVersion     uint64
		expectedRows        int
		expectedPendingNull int
	}{
		{
			name:            "nil then struct — struct latches object, nil flushes",
			rows:            []any{nil, testStruct{Name: "Alice"}},
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "struct then nil — struct latches object, nil appends empty JSON",
			rows:            []any{testStruct{Name: "Alice"}, nil},
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "nil then map — map latches object, nil flushes",
			rows:            []any{nil, map[string]any{"key": "value"}},
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "map then nil — map latches object",
			rows:            []any{map[string]any{"key": "value"}, nil},
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "nil then *chcol.JSON — *chcol.JSON latches object, nil flushes",
			rows:            []any{nil, chcol.NewJSON()},
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "*chcol.JSON then nil — *chcol.JSON latches object",
			rows:            []any{chcol.NewJSON(), nil},
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "nil then struct pointer — pointer-to-struct latches object",
			rows:            []any{nil, &testStruct{Name: "Bob"}},
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			// nil alone does NOT latch. It's
			// buffered as a pending null until a real row arrives or the
			// batch reaches WriteStatePrefix.
			name:                "nil only — defers (pending null, mode Unset)",
			rows:                []any{nil},
			expectedVersion:     jsonUnset,
			expectedRows:        1,
			expectedPendingNull: 1,
		},
		{
			name:            "multiple nils then struct — pendings flushed into object",
			rows:            []any{nil, nil, testStruct{Name: "Alice"}},
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    3,
		},
		{
			name:            "struct nil struct — object throughout",
			rows:            []any{testStruct{Name: "Alice"}, nil, testStruct{Name: "Bob"}},
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    3,
		},
		{
			// *interface{} with underlying nil is
			// also a null row (normalized by classifyJSONValue). Both rows
			// defer; mode stays Unset.
			name: "nil then pointer to nil interface — both defer",
			rows: func() []any {
				var s any
				return []any{s, &s}
			}(),
			expectedVersion:     jsonUnset,
			expectedRows:        2,
			expectedPendingNull: 2,
		},
		{
			name: "pointer to nil interface then nil — both defer",
			rows: func() []any {
				var s any
				return []any{&s, s}
			}(),
			expectedVersion:     jsonUnset,
			expectedRows:        2,
			expectedPendingNull: 2,
		},
		{
			name:            "nil then JSON string — string latches, nil flushes as \"null\"",
			rows:            []any{nil, `{"a":1}`},
			expectedVersion: JSONStringSerializationVersion,
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
				assert.Equal(t, tt.expectedPendingNull, col.pendingNullRows,
					"pendingNullRows mismatch — pending nulls should be 0 after a non-nil row latches the mode")
			}
			assert.Equal(t, tt.expectedVersion, col.serializationVersion)
		})
	}
}

func TestJSONAppendNilConsistency(t *testing.T) {
	type testStruct struct {
		Name string `json:"name"`
	}

	tests := []struct {
		name            string
		input           any
		wantErr         bool
		expectedVersion uint64
		expectedRows    int
	}{
		{
			name:            "nil then struct slice - should use object version",
			input:           []any{nil, testStruct{Name: "Alice"}},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "struct then nil slice - should use object version",
			input:           []any{testStruct{Name: "Alice"}, nil},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "nil then map slice - should use object version",
			input:           []any{nil, map[string]any{"key": "value"}},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "nil then *chcol.JSON slice - should use object version",
			input:           []any{nil, chcol.NewJSON()},
			wantErr:         false,
			expectedVersion: JSONObjectSerializationVersion,
			expectedRows:    2,
		},
		{
			// All-null batch: nil does not latch. Mode stays Unset after
			// Append; WriteStatePrefix latches String on encode and
			// flushes the pendings as "null" payloads.
			name:            "multiple nils slice - defers (mode Unset)",
			input:           []any{nil, nil, nil},
			wantErr:         false,
			expectedVersion: JSONUnsetSerializationVersion,
			expectedRows:    3,
		},
		{
			// []string is JSON text — must use string serialization, not
			// silently be stored as empty {} via the reflect.Value bug.
			name:            "[]string containing empty + json - should use string version",
			input:           []string{"", `{"a":1}`},
			wantErr:         false,
			expectedVersion: JSONStringSerializationVersion,
			expectedRows:    2,
		},
		{
			// []string is JSON text — string mode, period.
			name:            "pure string slice - should use string version",
			input:           []string{`{"a":1}`, `{"b":2}`},
			wantErr:         false,
			expectedVersion: JSONStringSerializationVersion,
			expectedRows:    2,
		},
		{
			// Mixed-mode inside one Append is not legal on the wire (one
			// serialization version per column per block). First element
			// latches string mode; the struct then rightly errors.
			// Previously this silently stored both rows as {}.
			name:    "mixed string + struct slice - must error, not silently drop data",
			input:   []any{`{"a":1}`, testStruct{Name: "Bob"}},
			wantErr: true,
		},
		{
			// All elements normalize to null — mode stays Unset.
			name: "nil + pointer-to-nil-interface slice - defers",
			input: func() []any {
				var v any
				return []any{v, &v}
			}(),
			wantErr:         false,
			expectedVersion: JSONUnsetSerializationVersion,
			expectedRows:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newTestJSONColumn(t)

			_, err := col.Append(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedRows, col.Rows())
			assert.Equal(t, tt.expectedVersion, col.serializationVersion)
		})
	}
}

func TestJSONAppendRowStringSerializationVersion(t *testing.T) {
	type testStruct struct {
		Name string `json:"name"`
	}

	tests := []struct {
		name            string
		rows            []any
		expectedVersion uint64
		expectedRows    int
	}{
		{
			name:            "string input - should use string version",
			rows:            []any{`{"name":"Alice"}`},
			expectedVersion: JSONStringSerializationVersion,
			expectedRows:    1,
		},
		{
			name:            "[]byte input - should use string version",
			rows:            []any{[]byte(`{"name":"Alice"}`)},
			expectedVersion: JSONStringSerializationVersion,
			expectedRows:    1,
		},
		{
			name:            "multiple strings - should use string version consistently",
			rows:            []any{`{"name":"Alice"}`, `{"name":"Bob"}`},
			expectedVersion: JSONStringSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "multiple []byte - should use string version consistently",
			rows:            []any{[]byte(`{"name":"Alice"}`), []byte(`{"name":"Bob"}`)},
			expectedVersion: JSONStringSerializationVersion,
			expectedRows:    2,
		},
		{
			name:            "empty string - should use string version",
			rows:            []any{``},
			expectedVersion: JSONStringSerializationVersion,
			expectedRows:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newTestJSONColumn(t)

			for _, row := range tt.rows {
				err := col.AppendRow(row)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectedRows, col.Rows())
			assert.Equal(t, tt.expectedVersion, col.serializationVersion)
		})
	}
}

// TestJSONAppendRowPointerAndStdlibStringTypes locks in the fix for the W&B
// report "*string stored as {}". Every value listed here must (a) latch
// string serialization and (b) round-trip through the jsonStrings backing
// column with the expected text. If any case regresses, the caller is
// silently losing data.
func TestJSONAppendRowPointerAndStdlibStringTypes(t *testing.T) {
	jsonText := `{"id":1,"name":"Book","tags":["a","b"]}`
	bs := []byte(jsonText)
	raw := json.RawMessage(jsonText)

	tests := []struct {
		name   string
		input  any
		stored string
	}{
		{"*string (Issue 1 repro)", &jsonText, jsonText},
		{"*[]byte", &bs, jsonText},
		{"json.RawMessage (explicit type-switch)", raw, jsonText},
		{"*json.RawMessage", &raw, jsonText},
		{"sql.NullString valid", sql.NullString{Valid: true, String: jsonText}, jsonText},
		// Invalid NullString is null-equivalent — stored as the JSON literal
		// "null" (see isNullishForJSON).
		{"sql.NullString invalid", sql.NullString{Valid: false}, "null"},
		{"*sql.NullString valid", &sql.NullString{Valid: true, String: jsonText}, jsonText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newTestJSONColumn(t)

			require.NoError(t, col.AppendRow(tt.input))
			require.Equal(t, 1, col.Rows())
			require.Equal(t, JSONStringSerializationVersion, col.serializationVersion,
				"value must latch string serialization — hitting the object path means data is being silently stored as {}")

			got := col.jsonStrings.Row(0, false)
			assert.Equal(t, tt.stored, got,
				"stored text must match the input — if this is {}, the *string regression is back")
		})
	}
}

// TestJSONAppendRowMixedModesAreRejected locks in the wire-format constraint:
// one serialization version per column per block. Once a column is latched
// (by an explicit object-mode or string-mode value), a row that would require
// the other mode must error, not silently become {}.
//
// Before the refactoring in #1850 nil itself could latch Object mode via a silent-{}
// fallback, which is why the old version of this test used nil as the first
// row. With nil now deferred, we latch explicitly — struct for object,
// string for string.
func TestJSONAppendRowMixedModesAreRejected(t *testing.T) {
	type goodStruct struct {
		Name string `json:"name"`
	}
	strPtr := func(s string) *string { return &s }

	tests := []struct {
		name          string
		latch         any
		wantLatch     uint64
		incompatible  any
		wantTypeInMsg string
	}{
		{
			name:          "struct latches object, then string row rejected",
			latch:         goodStruct{Name: "Alice"},
			wantLatch:     JSONObjectSerializationVersion,
			incompatible:  `{"should":"fail"}`,
			wantTypeInMsg: "string",
		},
		{
			name:          "struct latches object, then *string rejected",
			latch:         goodStruct{Name: "Alice"},
			wantLatch:     JSONObjectSerializationVersion,
			incompatible:  strPtr(`{"x":1}`),
			wantTypeInMsg: "string",
		},
		{
			name:          "struct latches object, then []byte rejected",
			latch:         goodStruct{Name: "Alice"},
			wantLatch:     JSONObjectSerializationVersion,
			incompatible:  []byte(`{"x":1}`),
			wantTypeInMsg: "string",
		},
		{
			name:          "map latches object, then int rejected",
			latch:         map[string]any{"name": "Alice"},
			wantLatch:     JSONObjectSerializationVersion,
			incompatible:  42,
			wantTypeInMsg: "int",
		},
		{
			// New test: string-first-then-struct was the biggest silent-data-
			// loss path in PR 1771. It must loudly error.
			name:          "string latches string, then struct rejected",
			latch:         `{"a":1}`,
			wantLatch:     JSONStringSerializationVersion,
			incompatible:  goodStruct{Name: "Alice"},
			wantTypeInMsg: "object",
		},
		{
			name:          "string latches string, then *chcol.JSON rejected",
			latch:         `{"a":1}`,
			wantLatch:     JSONStringSerializationVersion,
			incompatible:  chcol.NewJSON(),
			wantTypeInMsg: "object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newTestJSONColumn(t)

			require.NoError(t, col.AppendRow(tt.latch))
			require.Equal(t, tt.wantLatch, col.serializationVersion)
			rowsAfterLatch := col.Rows()

			err := col.AppendRow(tt.incompatible)
			require.Error(t, err, "mixed-mode row MUST return an error — silent {} would be the regression")
			assert.Contains(t, err.Error(), tt.wantTypeInMsg,
				"error message must describe the mode conflict so callers can fix their batch")
			assert.Equal(t, rowsAfterLatch, col.Rows(),
				"row count must not advance when AppendRow fails — partial writes corrupt batches")
		})
	}
}

// TestJSONAppendRowPointerToNilInterfaceDefers locks in that *any-with-nil is
// normalized to null by the classifier (same as plain nil), so it defers
// instead of latching any mode.
func TestJSONAppendRowPointerToNilInterfaceDefers(t *testing.T) {
	col := newTestJSONColumn(t)

	var s any // nil
	require.NoError(t, col.AppendRow(&s), "pointer to nil interface must be treated as a null row")
	require.Equal(t, 1, col.Rows())
	assert.Equal(t, 1, col.pendingNullRows)
	assert.Equal(t, JSONUnsetSerializationVersion, col.serializationVersion)
}

// TestJSONAllNullBatchEncodesAsStringWithNullPayload locks in the new
// encode-time contract for all-null batches: the column stays Unset through
// every AppendRow(nil), and at WriteStatePrefix it latches String mode and
// flushes the pending nulls as the JSON literal "null". This is the root fix
// for issue #1707 / #1768 — writing "" in String mode made the server reject
// the block with "Cannot parse JSON object here" (code 117).
func TestJSONAllNullBatchEncodesAsStringWithNullPayload(t *testing.T) {
	col := newTestJSONColumn(t)

	for i := 0; i < 3; i++ {
		require.NoError(t, col.AppendRow(nil))
	}
	require.Equal(t, 3, col.Rows())
	require.Equal(t, 3, col.pendingNullRows)
	require.Equal(t, JSONUnsetSerializationVersion, col.serializationVersion)

	// Trigger encode-time flush.
	buf := &proto.Buffer{}
	require.NoError(t, col.WriteStatePrefix(buf))

	require.Equal(t, JSONStringSerializationVersion, col.serializationVersion,
		"WriteStatePrefix must latch String for an all-null batch")
	require.Equal(t, 0, col.pendingNullRows, "pending nulls must be flushed after latching")
	require.Equal(t, 3, col.Rows(), "row count must be preserved across the flush")

	for i := 0; i < 3; i++ {
		assert.Equal(t, "null", col.jsonStrings.Row(i, false),
			"each flushed null must be the JSON literal \"null\" (NOT \"\" — that crashes the server)")
	}
}

func TestJSONNullAfterStringLatchEncodesAsNullLiteral(t *testing.T) {
	col := newTestJSONColumn(t)
	require.NoError(t, col.AppendRow(`{"a":1}`))
	require.Equal(t, JSONStringSerializationVersion, col.serializationVersion)
	require.NoError(t, col.AppendRow(nil))

	require.Equal(t, 2, col.Rows())
	assert.Equal(t, `{"a":1}`, col.jsonStrings.Row(0, false))
	assert.Equal(t, "null", col.jsonStrings.Row(1, false),
		"nil row after a String-mode latch must be the JSON literal \"null\"")
}

func TestJSONNullBeforeStringLatchFlushesAsNullLiteral(t *testing.T) {
	col := newTestJSONColumn(t)
	require.NoError(t, col.AppendRow(nil))
	require.Equal(t, 1, col.pendingNullRows)
	require.NoError(t, col.AppendRow(`{"a":1}`))

	require.Equal(t, JSONStringSerializationVersion, col.serializationVersion)
	require.Equal(t, 0, col.pendingNullRows)
	require.Equal(t, 2, col.Rows())
	assert.Equal(t, "null", col.jsonStrings.Row(0, false),
		"deferred null flushed on latch must be the JSON literal \"null\"")
	assert.Equal(t, `{"a":1}`, col.jsonStrings.Row(1, false))
}

func TestJSONAppendSliceRoundTripsData(t *testing.T) {
	type s struct {
		Name string `json:"name"`
	}
	jsonText := `{"id":1,"name":"Book"}`

	t.Run("[]struct creates dynamic path and populates it", func(t *testing.T) {
		// Before the #1850: appendObject's reflect iteration passed a
		// reflect.Value to AppendRow, which sent it through structToJSON.
		// reflect.Value is a struct with only unexported fields, so every
		// row became an empty chcol.JSON{} — no dynamic paths were created.
		// After the fix: the real struct is passed, its exported "Name"
		// field is picked up, and a dynamic path "name" appears.
		col := newTestJSONColumn(t)
		_, err := col.Append([]s{{Name: "Alice"}, {Name: "Bob"}})
		require.NoError(t, err)
		require.Equal(t, 2, col.Rows())
		require.Equal(t, JSONObjectSerializationVersion, col.serializationVersion)

		require.Contains(t, col.dynamicPaths, "name",
			"struct fields must create dynamic paths — empty dynamicPaths means the reflect.Value bug is back")

		nameIdx := col.dynamicPathsIndex["name"]
		require.Equal(t, 2, col.dynamicColumns[nameIdx].Rows(),
			"both struct rows' Name values must be present in the dynamic column")
	})

	t.Run("[]string latches string mode and stores text", func(t *testing.T) {
		col := newTestJSONColumn(t)
		_, err := col.Append([]string{jsonText, `{"x":2}`})
		require.NoError(t, err)
		require.Equal(t, 2, col.Rows())
		require.Equal(t, JSONStringSerializationVersion, col.serializationVersion,
			"[]string is JSON text — must use string serialization")
		assert.Equal(t, jsonText, col.jsonStrings.Row(0, false))
		assert.Equal(t, `{"x":2}`, col.jsonStrings.Row(1, false))
	})

	t.Run("[]*string stores dereferenced text and nil as JSON null", func(t *testing.T) {
		col := newTestJSONColumn(t)
		a, b := jsonText, `{"x":2}`
		_, err := col.Append([]*string{&a, nil, &b})
		require.NoError(t, err)
		require.Equal(t, 3, col.Rows())
		require.Equal(t, JSONStringSerializationVersion, col.serializationVersion)
		assert.Equal(t, jsonText, col.jsonStrings.Row(0, false))
		assert.Equal(t, "null", col.jsonStrings.Row(1, false))
		assert.Equal(t, `{"x":2}`, col.jsonStrings.Row(2, false))
	})

	t.Run("[]json.RawMessage stores text", func(t *testing.T) {
		col := newTestJSONColumn(t)
		_, err := col.Append([]json.RawMessage{json.RawMessage(jsonText)})
		require.NoError(t, err)
		require.Equal(t, 1, col.Rows())
		require.Equal(t, JSONStringSerializationVersion, col.serializationVersion)
		assert.Equal(t, jsonText, col.jsonStrings.Row(0, false))
	})

	t.Run("[]*json.RawMessage stores text and nil as JSON null", func(t *testing.T) {
		col := newTestJSONColumn(t)
		a := json.RawMessage(jsonText)
		b := json.RawMessage(`{"x":2}`)
		_, err := col.Append([]*json.RawMessage{&a, nil, &b})
		require.NoError(t, err)
		require.Equal(t, 3, col.Rows())
		require.Equal(t, JSONStringSerializationVersion, col.serializationVersion)
		assert.Equal(t, jsonText, col.jsonStrings.Row(0, false))
		assert.Equal(t, "null", col.jsonStrings.Row(1, false))
		assert.Equal(t, `{"x":2}`, col.jsonStrings.Row(2, false))
	})

	t.Run("[]sql.NullString stores text of Valid rows and JSON null for invalid rows", func(t *testing.T) {
		col := newTestJSONColumn(t)
		_, err := col.Append([]sql.NullString{
			{Valid: true, String: jsonText},
			{Valid: false},
		})
		require.NoError(t, err)
		require.Equal(t, 2, col.Rows())
		require.Equal(t, JSONStringSerializationVersion, col.serializationVersion)
		assert.Equal(t, jsonText, col.jsonStrings.Row(0, false))
		assert.Equal(t, "null", col.jsonStrings.Row(1, false))
	})

	t.Run("[]*sql.NullString stores text and null-equivalent pointers as JSON null", func(t *testing.T) {
		col := newTestJSONColumn(t)
		validA := &sql.NullString{Valid: true, String: jsonText}
		invalid := &sql.NullString{Valid: false}
		validB := &sql.NullString{Valid: true, String: `{"x":2}`}
		_, err := col.Append([]*sql.NullString{validA, nil, invalid, validB})
		require.NoError(t, err)
		require.Equal(t, 4, col.Rows())
		require.Equal(t, JSONStringSerializationVersion, col.serializationVersion)
		assert.Equal(t, jsonText, col.jsonStrings.Row(0, false))
		assert.Equal(t, "null", col.jsonStrings.Row(1, false))
		assert.Equal(t, "null", col.jsonStrings.Row(2, false))
		assert.Equal(t, `{"x":2}`, col.jsonStrings.Row(3, false))
	})
	t.Run("[]any containing structs populates dynamic paths", func(t *testing.T) {
		col := newTestJSONColumn(t)
		_, err := col.Append([]any{s{Name: "Alice"}, s{Name: "Bob"}})
		require.NoError(t, err)
		require.Equal(t, 2, col.Rows())
		require.Equal(t, JSONObjectSerializationVersion, col.serializationVersion)

		require.Contains(t, col.dynamicPaths, "name",
			"structs inside an []any must still flow through to dynamic paths")

		nameIdx := col.dynamicPathsIndex["name"]
		require.Equal(t, 2, col.dynamicColumns[nameIdx].Rows())
	})
}

// TestJSONAppendRejectsNonSlice locks in that Append is slice-oriented.
// The previous code had unreachable single-value type-switch cases (string,
// []byte routed to appendString which only accepts slices). Callers wanting
// a single-row insert should use AppendRow.
func TestJSONAppendRejectsNonSlice(t *testing.T) {
	tests := []struct {
		name string
		v    any
	}{
		{"single string", `{"a":1}`},
		{"single *string", func() *string { s := `{"a":1}`; return &s }()},
		{"single int", 42},
		{"single struct", struct{ Name string }{Name: "Alice"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newTestJSONColumn(t)
			_, err := col.Append(tt.v)
			require.Error(t, err,
				"Append is columnar — single values must be an error, not a silent 0-row success")
		})
	}
}
