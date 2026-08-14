package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBindQueryOrAppendParametersPrefersExplicitParameters(t *testing.T) {
	options := &QueryOptions{parameters: Parameters{"already": "set"}}
	query, err := bindQueryOrAppendParameters(true, options, "SELECT {name:String}", time.UTC, Named("name", "ignored"))
	require.NoError(t, err)
	assert.Equal(t, "SELECT {name:String}", query)
	// args are ignored entirely once parameters are already populated.
	assert.Equal(t, Parameters{"already": "set"}, options.parameters)
}

func TestBindQueryOrAppendParametersFallsBackWhenProtocolUnsupported(t *testing.T) {
	options := &QueryOptions{}
	query, err := bindQueryOrAppendParameters(false, options, "SELECT $1", time.UTC, 42)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 42", query)
	assert.Nil(t, options.parameters)
}

func TestBindQueryOrAppendParametersFallsBackWhenNoParamSyntax(t *testing.T) {
	options := &QueryOptions{}
	query, err := bindQueryOrAppendParameters(true, options, "SELECT $1", time.UTC, 42)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 42", query)
	assert.Nil(t, options.parameters)
}

func TestBindQueryOrAppendParametersFallsBackWhenNoArgs(t *testing.T) {
	options := &QueryOptions{}
	query, err := bindQueryOrAppendParameters(true, options, "SELECT {name:String}", time.UTC)
	require.NoError(t, err)
	assert.Equal(t, "SELECT {name:String}", query)
	assert.Nil(t, options.parameters)
}

func TestBindQueryOrAppendParametersNamedValue(t *testing.T) {
	str := "hello"
	tm := time.Unix(1700000000, 500_000_000)

	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{name: "nil value becomes escape marker", value: nil, expected: `\N`},
		{name: "string is sent raw and unquoted", value: "hello", expected: "hello"},
		{name: "*string is dereferenced and sent raw", value: &str, expected: "hello"},
		{name: "time.Time uses formatTimeParam", value: tm, expected: formatTimeParam(tm)},
		{name: "*time.Time uses formatTimeParam", value: &tm, expected: formatTimeParam(tm)},
		{name: "other types go through formatValue", value: 42, expected: "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := &QueryOptions{}
			query, err := bindQueryOrAppendParameters(true, options, "SELECT {p:String}", time.UTC, Named("p", tt.value))
			require.NoError(t, err)
			assert.Equal(t, "SELECT {p:String}", query)
			assert.Equal(t, tt.expected, options.parameters["p"])
		})
	}
}

func TestBindQueryOrAppendParametersNamedDateValue(t *testing.T) {
	tm := time.Unix(1700000000, 123_000_000)

	options := &QueryOptions{}
	_, err := bindQueryOrAppendParameters(true, options, "SELECT {p:DateTime64(3)}", time.UTC, DateNamed("p", tm, MilliSeconds))
	require.NoError(t, err)
	assert.Equal(t, formatTimeWithScale(tm, MilliSeconds), options.parameters["p"])
}

func TestBindQueryOrAppendParametersNamedDateValueZeroValue(t *testing.T) {
	options := &QueryOptions{}
	_, err := bindQueryOrAppendParameters(true, options, "SELECT {p:DateTime}", time.UTC, DateNamed("p", time.Time{}, Seconds))
	assert.ErrorIs(t, err, ErrInvalidValueInNamedDateValue)
}

func TestBindQueryOrAppendParametersNamedDateValueEmptyName(t *testing.T) {
	options := &QueryOptions{}
	_, err := bindQueryOrAppendParameters(true, options, "SELECT {p:DateTime}", time.UTC, DateNamed("", time.Now(), Seconds))
	assert.ErrorIs(t, err, ErrInvalidValueInNamedDateValue)
}

func TestBindQueryOrAppendParametersUnsupportedArgType(t *testing.T) {
	options := &QueryOptions{}
	_, err := bindQueryOrAppendParameters(true, options, "SELECT {p:Int32}", time.UTC, 42)
	assert.ErrorIs(t, err, ErrUnsupportedQueryParameter)
}

func TestIsNilParamValue(t *testing.T) {
	var nilInt *int
	notNilInt := 5

	tests := []struct {
		name     string
		value    any
		expected bool
	}{
		{name: "untyped nil", value: nil, expected: true},
		{name: "typed nil pointer", value: nilInt, expected: true},
		{name: "non-nil pointer", value: &notNilInt, expected: false},
		{name: "non-pointer zero value", value: 0, expected: false},
		{name: "empty string", value: "", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isNilParamValue(tt.value))
		})
	}
}

func TestFormatEpoch(t *testing.T) {
	tm := time.Unix(1700000000, 123_456_789)

	assert.Equal(t, "1700000000", formatEpoch(tm, 0))
	assert.Equal(t, "1700000000.123", formatEpoch(tm, 3))
	assert.Equal(t, "1700000000.123456", formatEpoch(tm, 6))
	assert.Equal(t, "1700000000.123456789", formatEpoch(tm, 9))
}

func TestFormatEpochBeforeUnixEpoch(t *testing.T) {
	// 1969-12-31T23:59:59.5Z decomposes as sec=-1, nsec=5e8 in Go's time
	// representation; formatEpoch must recombine that into "-0.5", not "-1.5".
	tm := time.Unix(-1, 500_000_000)

	assert.Equal(t, "-1", formatEpoch(tm, 0))
	assert.Equal(t, "-0.500", formatEpoch(tm, 3))
	assert.Equal(t, "-0.500000000", formatEpoch(tm, 9))
}
