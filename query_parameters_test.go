package clickhouse

import (
	"net/netip"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBindQueryOrAppendParameters(t *testing.T) {
	cases := []struct {
		name            string
		protocolSupport bool
		options         *QueryOptions
		query           string
		args            []any
		wantQuery       string
		wantParameters  Parameters
	}{
		// args are ignored entirely once parameters are already populated.
		{"prefers explicit parameters over args", true, &QueryOptions{parameters: Parameters{"already": "set"}},
			"SELECT {name:String}", []any{Named("name", "ignored")}, "SELECT {name:String}", Parameters{"already": "set"}},
		{"falls back to bind when protocol unsupported", false, &QueryOptions{},
			"SELECT $1", []any{42}, "SELECT 42", nil},
		{"falls back to bind when query has no param syntax", true, &QueryOptions{},
			"SELECT $1", []any{42}, "SELECT 42", nil},
		{"falls back to bind when no args given", true, &QueryOptions{},
			"SELECT {name:String}", nil, "SELECT {name:String}", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			query, err := bindQueryOrAppendParameters(tc.protocolSupport, tc.options, tc.query, time.UTC, tc.args...)
			require.NoError(t, err)
			assert.Equal(t, tc.wantQuery, query)
			assert.Equal(t, tc.wantParameters, tc.options.parameters)
		})
	}
}

func TestBindQueryOrAppendParametersNamedValue(t *testing.T) {
	str := "hello"
	tm := time.Unix(1700000000, 500_000_000)
	addr := netip.MustParseAddr("10.0.0.1")

	cases := []struct {
		name  string
		value any
		want  string
	}{
		{"nil value becomes escape marker", nil, `\N`},
		{"string is TSV-escaped and unquoted", "hello", "hello"},
		{"*string is dereferenced and TSV-escaped", &str, "hello"},
		{"time.Time uses formatTimeParam", tm, "1700000000.500"},
		{"*time.Time uses formatTimeParam", &tm, "1700000000.500"},
		{"time.Duration keeps its own text, not String()", 90 * time.Minute, "01:30:00"},
		{"fmt.Stringer is sent raw and unquoted", uuid.MustParse("11111111-1111-1111-1111-111111111111"), "11111111-1111-1111-1111-111111111111"},
		{"fmt.Stringer pointer is sent raw and unquoted", &addr, "10.0.0.1"},
		{"other types go through formatValue", 42, "42"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			options := &QueryOptions{}
			query, err := bindQueryOrAppendParameters(true, options, "SELECT {p:String}", time.UTC, Named("p", tc.value))
			require.NoError(t, err)
			assert.Equal(t, "SELECT {p:String}", query)
			assert.Equal(t, tc.want, options.parameters["p"])
		})
	}
}

func TestBindQueryOrAppendParametersNestedStringerStaysQuoted(t *testing.T) {
	cases := []struct {
		name  string
		query string
		value any
		want  string
	}{
		{"array element", "SELECT {p:Array(UUID)}", []uuid.UUID{uuid.MustParse("11111111-1111-1111-1111-111111111111")}, "['11111111-1111-1111-1111-111111111111']"},
		{"tuple element", "SELECT {p:Tuple(UUID, UInt8)}", GroupSet{Value: []any{uuid.MustParse("11111111-1111-1111-1111-111111111111"), uint8(1)}}, "('11111111-1111-1111-1111-111111111111', 1)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			options := &QueryOptions{}
			_, err := bindQueryOrAppendParameters(true, options, tc.query, time.UTC, Named("p", tc.value))
			require.NoError(t, err)
			assert.Equal(t, tc.want, options.parameters["p"])
		})
	}
}

// TestNamedStringEscaper checks that control characters in a raw Named string
// are TSV-escaped once at the point of entry. The same representation works on
// both protocols: HTTP feeds it straight into the URL query string (the server
// decodes it via deserializeTextEscaped once), and TCP re-escapes the backslashes
// in encodeFieldDump so the escapes survive readQuoted and are then decoded by
// deserializeTextEscaped.
func TestNamedStringEscaper(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  string
	}{
		{"plain string", "hello", "hello"},
		{"tab", "a\tb", `a\tb`},
		{"newline", "a\nb", `a\nb`},
		{"carriage return", "a\rb", `a\rb`},
		{"backslash", `a\b`, `a\\b`},
		{"backslash before n is preserved", `a\nb`, `a\\nb`},
		{"single quote left to the boundary", "it's", "it's"},
		{"NUL byte", "a\x00b", `a\0b`},
		{"mixed", "t:\tx\nnewline\\backslash'quote", `t:\tx\nnewline\\backslash'quote`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			options := &QueryOptions{}
			_, err := bindQueryOrAppendParameters(true, options, "SELECT {p:String}", time.UTC, Named("p", tc.value))
			require.NoError(t, err)
			assert.Equal(t, tc.want, options.parameters["p"])
		})
	}
}

func TestBindQueryOrAppendParametersNamedDateValue(t *testing.T) {
	tm := time.Unix(1700000000, 123_000_000)

	options := &QueryOptions{}
	_, err := bindQueryOrAppendParameters(true, options, "SELECT {p:DateTime64(3)}", time.UTC, DateNamed("p", tm, MilliSeconds))
	require.NoError(t, err)
	assert.Equal(t, "1700000000.123", options.parameters["p"])
}

func TestBindQueryOrAppendParametersErrors(t *testing.T) {
	cases := []struct {
		name  string
		arg   any
		query string
		want  error
	}{
		{"NamedDateValue with zero value", DateNamed("p", time.Time{}, Seconds), "SELECT {p:DateTime}", ErrInvalidValueInNamedDateValue},
		{"NamedDateValue with empty name", DateNamed("", time.Now(), Seconds), "SELECT {p:DateTime}", ErrInvalidValueInNamedDateValue},
		{"unsupported arg type", 42, "SELECT {p:Int32}", ErrUnsupportedQueryParameter},
		{"unsupported arg type beside literal with braces", 42, `SELECT '{"a":1}', {p:Int32}`, ErrUnsupportedQueryParameter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			options := &QueryOptions{}
			_, err := bindQueryOrAppendParameters(true, options, tc.query, time.UTC, tc.arg)
			assert.ErrorIs(t, err, tc.want)
		})
	}
}

func TestIsNilParamValue(t *testing.T) {
	var nilInt *int
	notNilInt := 5

	cases := []struct {
		name  string
		value any
		want  bool
	}{
		{"untyped nil", nil, true},
		{"typed nil pointer", nilInt, true},
		{"non-nil pointer", &notNilInt, false},
		{"non-pointer zero value", 0, false},
		{"empty string", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isNilParamValue(tc.value))
		})
	}
}

func TestFormatEpoch(t *testing.T) {
	tm := time.Unix(1700000000, 123_456_789)

	cases := []struct {
		name   string
		digits int
		want   string
	}{
		{"whole seconds", 0, "1700000000"},
		{"milliseconds", 3, "1700000000.123"},
		{"microseconds", 6, "1700000000.123456"},
		{"nanoseconds", 9, "1700000000.123456789"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatEpoch(tm, tc.digits))
		})
	}
}

func TestFormatEpochBeforeUnixEpoch(t *testing.T) {
	// 1969-12-31T23:59:59.5Z decomposes as sec=-1, nsec=5e8 in Go's time
	// representation; formatEpoch must recombine that into "-0.5", not "-1.5".
	tm := time.Unix(-1, 500_000_000)

	cases := []struct {
		name   string
		digits int
		want   string
	}{
		{"whole seconds", 0, "-1"},
		{"milliseconds", 3, "-0.500"},
		{"nanoseconds", 9, "-0.500000000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatEpoch(tm, tc.digits))
		})
	}
}

func TestBindQueryOrAppendParametersDetection(t *testing.T) {
	cases := []struct {
		name           string
		query          string
		args           []any
		wantServerSide bool
		wantQuery      string
		wantParams     Parameters
	}{
		// Issue #1489: braces and colons inside string literals must not trigger server-side mode.
		{
			name:           "#1489 literals with braces and colon on one line",
			query:          "SELECT '{' as one, ':' as two, '}' as three WHERE ?",
			args:           []any{1},
			wantServerSide: false,
			wantQuery:      "SELECT '{' as one, ':' as two, '}' as three WHERE 1",
		},
		{
			name:           "#1489 concat literals with braces and colon",
			query:          "SELECT concat('{', ':', '}') FROM issue1489 WHERE id = ?",
			args:           []any{2},
			wantServerSide: false,
			wantQuery:      "SELECT concat('{', ':', '}') FROM issue1489 WHERE id = 2",
		},
		{
			name:           "#1489 jsonMergePatch on one line",
			query:          `SELECT jsonMergePatch('{"test":' || test_value || '}') FROM some_table WHERE id = ?`,
			args:           []any{42},
			wantServerSide: false,
			wantQuery:      `SELECT jsonMergePatch('{"test":' || test_value || '}') FROM some_table WHERE id = 42`,
		},
		{
			name:           "#1489 jsonMergePatch multiline",
			query:          "SELECT jsonMergePatch('{\"test\":' || test_value ||\n   '}') FROM some_table WHERE id = ?",
			args:           []any{42},
			wantServerSide: false,
			wantQuery:      "SELECT jsonMergePatch('{\"test\":' || test_value ||\n   '}') FROM some_table WHERE id = 42",
		},
		{
			name:           "#1489 json object literal in WHERE with ?",
			query:          `SELECT count() FROM numbers(?) WHERE '{"a":1}' != ''`,
			args:           []any{uint32(5)},
			wantServerSide: false,
			wantQuery:      `SELECT count() FROM numbers(5) WHERE '{"a":1}' != ''`,
		},
		{
			name:           "json object literal with numeric $1",
			query:          `SELECT '{"a":1}' WHERE id = $1`,
			args:           []any{42},
			wantServerSide: false,
			wantQuery:      `SELECT '{"a":1}' WHERE id = 42`,
		},
		{
			name:           "escaped single quote before placeholder-like text in literal",
			query:          `SELECT 'it\'s {id:UInt32}' WHERE id = ?`,
			args:           []any{1},
			wantServerSide: false,
			wantQuery:      `SELECT 'it\'s {id:UInt32}' WHERE id = 1`,
		},
		{
			name:           "doubled single quote before placeholder-like text in literal",
			query:          "SELECT 'it''s {id:UInt32}' WHERE id = ?",
			args:           []any{1},
			wantServerSide: false,
			wantQuery:      "SELECT 'it''s {id:UInt32}' WHERE id = 1",
		},

		// Issue #1828: {name:Type} inside SQL comments must not trigger server-side mode.
		{
			name:           "#1828 matching name in -- comment with @name",
			query:          "-- server-side: {limit:UInt32}\nSELECT count() FROM numbers(@limit)",
			args:           []any{Named("limit", uint32(5))},
			wantServerSide: false,
			wantQuery:      "-- server-side: {limit:UInt32}\nSELECT count() FROM numbers(5)",
		},
		{
			name:           "#1828 other name in -- comment with @name",
			query:          "-- see {name:Type}\nSELECT count() FROM numbers(@limit)",
			args:           []any{Named("limit", uint32(5))},
			wantServerSide: false,
			wantQuery:      "-- see {name:Type}\nSELECT count() FROM numbers(5)",
		},
		{
			name:           "#1828 {name:Type} in /* */ comment with @name",
			query:          "/* {limit:UInt32} */ SELECT count() FROM numbers(@limit)",
			args:           []any{Named("limit", uint32(5))},
			wantServerSide: false,
			wantQuery:      "/* {limit:UInt32} */ SELECT count() FROM numbers(5)",
		},
		{
			name:           "#1828 {name:Type} in -- comment with ?",
			query:          "-- see {name:Type}\nSELECT count() FROM numbers(?)",
			args:           []any{uint32(5)},
			wantServerSide: false,
			wantQuery:      "-- see {name:Type}\nSELECT count() FROM numbers(5)",
		},
		{
			name:           "{name:Type} in # line comment with ?",
			query:          "# {limit:UInt32}\nSELECT count() FROM numbers(?)",
			args:           []any{uint32(5)},
			wantServerSide: false,
			wantQuery:      "# {limit:UInt32}\nSELECT count() FROM numbers(5)",
		},
		{
			name:           "{name:Type} in #! line comment with ?",
			query:          "#! {limit:UInt32}\nSELECT count() FROM numbers(?)",
			args:           []any{uint32(5)},
			wantServerSide: false,
			wantQuery:      "#! {limit:UInt32}\nSELECT count() FROM numbers(5)",
		},
		{
			name:           "{name:Type} in nested /* /* */ */ block comment with ?",
			query:          "/* outer /* {a:Int32} */ {b:String} */ SELECT count() FROM numbers(?)",
			args:           []any{uint32(5)},
			wantServerSide: false,
			wantQuery:      "/* outer /* {a:Int32} */ {b:String} */ SELECT count() FROM numbers(5)",
		},

		// Quoted identifiers must not trigger server-side mode.
		{
			name:           "{name:Type} inside backtick identifier",
			query:          "SELECT `{id:UInt32}` FROM t WHERE id = ?",
			args:           []any{7},
			wantServerSide: false,
			wantQuery:      "SELECT `{id:UInt32}` FROM t WHERE id = 7",
		},
		{
			name:           "{name:Type} after doubled backtick inside identifier",
			query:          "SELECT `col``{id:UInt32}` FROM t WHERE id = ?",
			args:           []any{7},
			wantServerSide: false,
			wantQuery:      "SELECT `col``{id:UInt32}` FROM t WHERE id = 7",
		},
		{
			name:           "{name:Type} inside double-quoted identifier",
			query:          `SELECT "{id:UInt32}" FROM t WHERE id = ?`,
			args:           []any{7},
			wantServerSide: false,
			wantQuery:      `SELECT "{id:UInt32}" FROM t WHERE id = 7`,
		},
		{
			name:           "{name:Type} after doubled double-quote inside identifier",
			query:          `SELECT "col""{id:UInt32}" FROM t WHERE id = ?`,
			args:           []any{7},
			wantServerSide: false,
			wantQuery:      `SELECT "col""{id:UInt32}" FROM t WHERE id = 7`,
		},

		// Malformed or non-identifier braces outside quotes/comments must not trigger server-side mode.
		{
			name:           "numeric key {1:2} is not a server-side parameter",
			query:          "SELECT {1:2} WHERE id = ?",
			args:           []any{1},
			wantServerSide: false,
			wantQuery:      "SELECT {1:2} WHERE id = 1",
		},
		{
			name:           "empty braces {}",
			query:          "SELECT {} WHERE id = ?",
			args:           []any{1},
			wantServerSide: false,
			wantQuery:      "SELECT {} WHERE id = 1",
		},
		{
			name:           "missing name {:String}",
			query:          "SELECT {:String} WHERE id = ?",
			args:           []any{1},
			wantServerSide: false,
			wantQuery:      "SELECT {:String} WHERE id = 1",
		},
		{
			name:           "missing type {id:}",
			query:          "SELECT {id:} WHERE id = ?",
			args:           []any{1},
			wantServerSide: false,
			wantQuery:      "SELECT {id:} WHERE id = 1",
		},
		{
			name:           "nested non-parameter {a:{b}}",
			query:          "SELECT {a:{b}} WHERE id = ?",
			args:           []any{1},
			wantServerSide: false,
			wantQuery:      "SELECT {a:{b}} WHERE id = 1",
		},
		{
			name:           "comment-only type {id: /* c */}",
			query:          "SELECT {id: /* c */} WHERE id = ?",
			args:           []any{1},
			wantServerSide: false,
			wantQuery:      "SELECT {id: /* c */} WHERE id = 1",
		},
		{
			name:           "unclosed brace {id:UInt32",
			query:          "SELECT {id:UInt32 WHERE id = ?",
			args:           []any{1},
			wantServerSide: false,
			wantQuery:      "SELECT {id:UInt32 WHERE id = 1",
		},

		// Positive cases: genuine server-side placeholders must still select server-side parameters.
		{
			name:           "single server-side placeholder {id:UInt32}",
			query:          "SELECT {id:UInt32}",
			args:           []any{Named("id", uint32(7))},
			wantServerSide: true,
			wantQuery:      "SELECT {id:UInt32}",
			wantParams:     Parameters{"id": "7"},
		},
		{
			name:           "multiple server-side placeholders",
			query:          "SELECT {id:UInt32}, {name:String}",
			args:           []any{Named("id", uint32(7)), Named("name", "alice")},
			wantServerSide: true,
			wantQuery:      "SELECT {id:UInt32}, {name:String}",
			wantParams:     Parameters{"id": "7", "name": "alice"},
		},
		{
			name:           "literal with braces before real placeholder",
			query:          `SELECT '{"a":1}', {id:UInt32}`,
			args:           []any{Named("id", uint32(7))},
			wantServerSide: true,
			wantQuery:      `SELECT '{"a":1}', {id:UInt32}`,
			wantParams:     Parameters{"id": "7"},
		},
		{
			name:           "real placeholder before literal with braces",
			query:          `SELECT {id:UInt32}, jsonMergePatch('{"test":1}')`,
			args:           []any{Named("id", uint32(7))},
			wantServerSide: true,
			wantQuery:      `SELECT {id:UInt32}, jsonMergePatch('{"test":1}')`,
			wantParams:     Parameters{"id": "7"},
		},
		{
			name:           "comment with placeholder before real placeholder",
			query:          "-- {ignored:String}\nSELECT {id:UInt32}",
			args:           []any{Named("id", uint32(7))},
			wantServerSide: true,
			wantQuery:      "-- {ignored:String}\nSELECT {id:UInt32}",
			wantParams:     Parameters{"id": "7"},
		},
		{
			name:           "parameterized type with single quotes DateTime64(3, 'UTC')",
			query:          "SELECT {ts:DateTime64(3, 'UTC')}",
			args:           []any{Named("ts", "2026-01-01 00:00:00.000")},
			wantServerSide: true,
			wantQuery:      "SELECT {ts:DateTime64(3, 'UTC')}",
			wantParams:     Parameters{"ts": "2026-01-01 00:00:00.000"},
		},
		{
			name:           "Enum type with brace inside quoted enum name",
			query:          "SELECT {e:Enum8('}' = 1, '{' = 2)}",
			args:           []any{Named("e", "}")},
			wantServerSide: true,
			wantQuery:      "SELECT {e:Enum8('}' = 1, '{' = 2)}",
			wantParams:     Parameters{"e": "}"},
		},
		{
			name:           "whitespace around placeholder name and type",
			query:          "SELECT { value : Int64 }",
			args:           []any{Named("value", int64(9))},
			wantServerSide: true,
			wantQuery:      "SELECT { value : Int64 }",
			wantParams:     Parameters{"value": "9"},
		},
		{
			name:           "dollar sign in parameter name",
			query:          "SELECT {id$x:Int32}",
			args:           []any{Named("id$x", int32(42))},
			wantServerSide: true,
			wantQuery:      "SELECT {id$x:Int32}",
			wantParams:     Parameters{"id$x": "42"},
		},
		{
			name:           "unclosed outer brace before valid placeholder",
			query:          "SELECT {broken, {id:UInt32}",
			args:           []any{Named("id", uint32(7))},
			wantServerSide: true,
			wantQuery:      "SELECT {broken, {id:UInt32}",
			wantParams:     Parameters{"id": "7"},
		},
		{
			name:           "unclosed outer param prefix before valid placeholder",
			query:          "SELECT {outer: {id:UInt32}",
			args:           []any{Named("id", uint32(7))},
			wantServerSide: true,
			wantQuery:      "SELECT {outer: {id:UInt32}",
			wantParams:     Parameters{"id": "7"},
		},
		{
			name:           "multiline Tuple placeholder",
			query:          "SELECT {p:Tuple(\n    a String,\n    b Int32\n)}",
			args:           []any{Named("p", "('x', 1)")},
			wantServerSide: true,
			wantQuery:      "SELECT {p:Tuple(\n    a String,\n    b Int32\n)}",
			wantParams:     Parameters{"p": "('x', 1)"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			options := &QueryOptions{}
			query, err := bindQueryOrAppendParameters(true, options, tc.query, time.UTC, tc.args...)
			require.NoError(t, err)
			assert.Equal(t, tc.wantQuery, query)
			if tc.wantServerSide {
				assert.Equal(t, tc.wantParams, options.parameters)
			} else {
				assert.Nil(t, options.parameters)
			}
		})
	}
}
