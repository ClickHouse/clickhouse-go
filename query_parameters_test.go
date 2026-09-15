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

func TestBindQueryIgnoresParameterSyntaxInProtectedContexts(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "string",
			query:    `SELECT '{"key":"value"}', ?`,
			expected: `SELECT '{"key":"value"}', 42`,
		},
		{
			name:     "block comment",
			query:    `SELECT ? /* {fake:UInt64} */`,
			expected: `SELECT 42 /* {fake:UInt64} */`,
		},
		{
			name:     "line comment",
			query:    "SELECT ? -- {fake:UInt64}\n",
			expected: "SELECT 42 -- {fake:UInt64}\n",
		},
		{
			name:     "double quoted identifier",
			query:    `SELECT "{fake:UInt64}", ?`,
			expected: `SELECT "{fake:UInt64}", 42`,
		},
		{
			name:     "backtick identifier",
			query:    "SELECT `{fake:UInt64}`, ?",
			expected: "SELECT `{fake:UInt64}`, 42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := QueryOptions{}
			actual, err := bindQueryOrAppendParameters(true, &options, tt.query, time.UTC, 42)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
			assert.Empty(t, options.parameters)
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

func TestBindQueryDetectsQueryParameter(t *testing.T) {
	t.Run("quoted enum type", func(t *testing.T) {
		options := QueryOptions{}
		query := `SELECT {value:Enum8('enabled' = 1, 'disabled' = 0)}`

		actual, err := bindQueryOrAppendParameters(true, &options, query, time.UTC, Named("value", 1))

		require.NoError(t, err)
		assert.Equal(t, query, actual)
		assert.Equal(t, Parameters{"value": "1"}, options.parameters)
	})

	t.Run("whitespace around name and type", func(t *testing.T) {
		options := QueryOptions{}
		query := `SELECT { value : String }`

		actual, err := bindQueryOrAppendParameters(true, &options, query, time.UTC, Named("value", "hello"))

		require.NoError(t, err)
		assert.Equal(t, query, actual)
		assert.Equal(t, Parameters{"value": "hello"}, options.parameters)
	})

	t.Run("map literal with a positional argument", func(t *testing.T) {
		options := QueryOptions{}
		query := `SELECT {1:'a'}, ?`

		actual, err := bindQueryOrAppendParameters(true, &options, query, time.UTC, 42)

		require.NoError(t, err)
		assert.Equal(t, `SELECT {1:'a'}, 42`, actual)
		assert.Empty(t, options.parameters)
	})
}
