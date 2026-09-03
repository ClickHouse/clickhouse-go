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
		{"string is sent raw and unquoted", "hello", "hello"},
		{"*string is dereferenced and sent raw", &str, "hello"},
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
