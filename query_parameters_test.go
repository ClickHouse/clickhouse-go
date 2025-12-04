package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasParameterWrappers(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected bool
	}{
		{
			name:     "no parameters",
			args:     []any{1, "test", true},
			expected: false,
		},
		{
			name:     "all parameters",
			args:     []any{Param(1, "Int64"), Param("test", "String")},
			expected: true,
		},
		{
			name:     "mixed parameters",
			args:     []any{1, Param("test", "String"), true},
			expected: true,
		},
		{
			name:     "empty args",
			args:     []any{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasParameterWrappers(tt.args...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertPositionalToServerSide(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		args          []any
		expectedQuery string
		expectedErr   bool
	}{
		{
			name:          "single positional parameter",
			query:         "SELECT * FROM test WHERE id = ?",
			args:          []any{Param(123, "Int64")},
			expectedQuery: "SELECT * FROM test WHERE id = {param_1:Int64}",
			expectedErr:   false,
		},
		{
			name:          "multiple positional parameters",
			query:         "SELECT * FROM test WHERE id = ? AND name = ?",
			args:          []any{Param(123, "Int64"), Param("John", "String")},
			expectedQuery: "SELECT * FROM test WHERE id = {param_1:Int64} AND name = {param_2:String}",
			expectedErr:   false,
		},
		{
			name:          "escaped question mark",
			query:         "SELECT * FROM test WHERE id = ? AND text LIKE '\\?'",
			args:          []any{Param(123, "Int64")},
			expectedQuery: "SELECT * FROM test WHERE id = {param_1:Int64} AND text LIKE '\\?'",
			expectedErr:   false,
		},
		{
			name:        "not all args are Parameter types",
			query:       "SELECT * FROM test WHERE id = ?",
			args:        []any{123},
			expectedErr: true,
		},
		{
			name:        "too few arguments",
			query:       "SELECT * FROM test WHERE id = ? AND name = ?",
			args:        []any{Param(123, "Int64")},
			expectedErr: true,
		},
		{
			name:        "too many arguments",
			query:       "SELECT * FROM test WHERE id = ?",
			args:        []any{Param(123, "Int64"), Param("extra", "String")},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := make(Parameters)
			query, _, err := convertPositionalToServerSide(tt.query, time.UTC, params, tt.args...)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedQuery, query)
			}
		})
	}
}

func TestConvertNumericToServerSide(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		args          []any
		expectedQuery string
		expectedErr   bool
	}{
		{
			name:          "single numeric parameter",
			query:         "SELECT * FROM test WHERE id = $1",
			args:          []any{Param(123, "Int64")},
			expectedQuery: "SELECT * FROM test WHERE id = {param_1:Int64}",
			expectedErr:   false,
		},
		{
			name:          "multiple numeric parameters",
			query:         "SELECT * FROM test WHERE id = $1 AND name = $2",
			args:          []any{Param(123, "Int64"), Param("John", "String")},
			expectedQuery: "SELECT * FROM test WHERE id = {param_1:Int64} AND name = {param_2:String}",
			expectedErr:   false,
		},
		{
			name:          "reordered numeric parameters",
			query:         "SELECT * FROM test WHERE name = $2 AND id = $1",
			args:          []any{Param(123, "Int64"), Param("John", "String")},
			expectedQuery: "SELECT * FROM test WHERE name = {param_2:String} AND id = {param_1:Int64}",
			expectedErr:   false,
		},
		{
			name:          "repeated numeric parameter",
			query:         "SELECT * FROM test WHERE id = $1 AND other_id = $1",
			args:          []any{Param(123, "Int64")},
			expectedQuery: "SELECT * FROM test WHERE id = {param_1:Int64} AND other_id = {param_1:Int64}",
			expectedErr:   false,
		},
		{
			name:        "not all args are Parameter types",
			query:       "SELECT * FROM test WHERE id = $1",
			args:        []any{123},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := make(Parameters)
			query, _, err := convertNumericToServerSide(tt.query, time.UTC, params, tt.args...)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedQuery, query)
			}
		})
	}
}

func TestConvertToServerSideBinding(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		args          []any
		expectedQuery string
		expectedErr   bool
		checkParams   bool
		expectedParam string
	}{
		{
			name:          "positional with string",
			query:         "SELECT * FROM test WHERE name = ?",
			args:          []any{Param("test'value", "String")},
			expectedQuery: "SELECT * FROM test WHERE name = {param_1:String}",
			expectedErr:   false,
			checkParams:   true,
			expectedParam: "'test\\'value'",
		},
		{
			name:          "numeric with int",
			query:         "SELECT * FROM test WHERE id = $1",
			args:          []any{Param(42, "Int64")},
			expectedQuery: "SELECT * FROM test WHERE id = {param_1:Int64}",
			expectedErr:   false,
			checkParams:   true,
			expectedParam: "42",
		},
		{
			name:        "mixed placeholders",
			query:       "SELECT * FROM test WHERE id = $1 AND name = ?",
			args:        []any{Param(123, "Int64"), Param("test", "String")},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, params, err := convertToServerSideBinding(tt.query, time.UTC, tt.args...)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedQuery, query)

				if tt.checkParams {
					assert.NotEmpty(t, params)
					// Check first parameter value
					assert.Equal(t, tt.expectedParam, params["param_1"])
				}
			}
		})
	}
}

func TestBindQueryOrAppendParametersWithServerSideConversion(t *testing.T) {
	tests := []struct {
		name               string
		paramsProtocolSupport bool
		query              string
		args               []any
		expectedQuery      string
		expectedErr        bool
		hasParameters      bool
	}{
		{
			name:               "convert positional to server-side",
			paramsProtocolSupport: true,
			query:              "SELECT * FROM test WHERE id = ?",
			args:               []any{Param(123, "Int64")},
			expectedQuery:      "SELECT * FROM test WHERE id = {param_1:Int64}",
			expectedErr:        false,
			hasParameters:      true,
		},
		{
			name:               "convert numeric to server-side",
			paramsProtocolSupport: true,
			query:              "SELECT * FROM test WHERE id = $1 AND name = $2",
			args:               []any{Param(123, "Int64"), Param("John", "String")},
			expectedQuery:      "SELECT * FROM test WHERE id = {param_1:Int64} AND name = {param_2:String}",
			expectedErr:        false,
			hasParameters:      true,
		},
		{
			name:               "no conversion when protocol doesn't support",
			paramsProtocolSupport: false,
			query:              "SELECT * FROM test WHERE id = ?",
			args:               []any{123},
			expectedQuery:      "SELECT * FROM test WHERE id = 123",
			expectedErr:        false,
			hasParameters:      false,
		},
		{
			name:               "no conversion for non-Parameter args",
			paramsProtocolSupport: true,
			query:              "SELECT * FROM test WHERE id = ?",
			args:               []any{123},
			expectedQuery:      "SELECT * FROM test WHERE id = 123",
			expectedErr:        false,
			hasParameters:      false,
		},
		{
			name:               "no conversion when query already has {name:Type} syntax",
			paramsProtocolSupport: true,
			query:              "SELECT * FROM test WHERE id = {id:Int64}",
			args:               []any{Param(123, "Int64")},
			expectedQuery:      "SELECT * FROM test WHERE id = {id:Int64}",
			expectedErr:        true, // Will fail because Parameter type is not supported in existing server-side binding
			hasParameters:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := &QueryOptions{
				settings: make(Settings),
			}

			query, err := bindQueryOrAppendParameters(tt.paramsProtocolSupport, options, tt.query, time.UTC, tt.args...)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedQuery, query)

				if tt.hasParameters {
					assert.NotEmpty(t, options.parameters, "Expected parameters to be populated")
				}
			}
		})
	}
}

func TestParamFunction(t *testing.T) {
	p := Param(123, "Int64")
	assert.Equal(t, 123, p.Value)
	assert.Equal(t, "Int64", p.CHType)

	p2 := Param("test", "String")
	assert.Equal(t, "test", p2.Value)
	assert.Equal(t, "String", p2.CHType)
}
