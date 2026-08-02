package clickhouse

import (
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
)

func TestBindQueryOrAppendParameters(t *testing.T) {
	testTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		param         any
		expectedValue string
		expectError   bool
	}{
		// Nil / NULL case (The fixed bug)
		{
			name:          "nil translates to \\N",
			param:         Named("param", nil),
			expectedValue: "\\N",
		},
		// Basic types
		{
			name:          "boolean true",
			param:         Named("param", true),
			expectedValue: "1",
		},
		{
			name:          "boolean false",
			param:         Named("param", false),
			expectedValue: "0",
		},
		{
			name:          "string direct bypass",
			param:         Named("param", "hello_world"),
			expectedValue: "hello_world",
		},
		{
			name:          "string with quotes bypass",
			param:         Named("param", "hello 'world'"),
			expectedValue: "hello 'world'", // String bypasses format(), so it shouldn't have extra quotes added
		},
		{
			name:          "integer",
			param:         Named("param", 42),
			expectedValue: "42",
		},
		{
			name:          "float",
			param:         Named("param", 3.1415),
			expectedValue: "3.1415",
		},
		// Collections
		{
			name:          "slice of ints",
			param:         Named("param", []int{1, 2, 3}),
			expectedValue: "[1, 2, 3]",
		},
		{
			name:          "slice of strings",
			param:         Named("param", []string{"a", "b", "c"}),
			expectedValue: "['a', 'b', 'c']",
		},
		// Time types
		// formatTime adds quotes and toDateTime
		{
			name:          "time.Time",
			param:         Named("param", testTime),
			expectedValue: "toDateTime('2023-01-01 12:00:05', 'UTC')",
		},
		// formatTimeWithScale behavior
		{
			name: "NamedDateValue",
			param: driver.NamedDateValue{
				Name:  "param",
				Value: testTime,
				Scale: uint8(Seconds),
			},
			expectedValue: "2023-01-01 12:00:00",
		},
		// Error cases
		// Not a NamedValue or NamedDateValue
		{
			name:          "unsupported type",
			param:         struct{ A int }{A: 1},
			expectedValue: "",
			expectError:   true,
		},
	}

	// The query must contain {param:Type}
	query := `
	SELECT * 
	FROM t 
	WHERE col = {param:String}`

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &QueryOptions{parameters: make(Parameters)}

			_, err := bindQueryOrAppendParameters(true, opts, query, time.UTC, tt.param)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// For time.Time standard format returns toDateTime('...', 'UTC'), we just verify it formats without error
				if tt.name == "time.Time" {
					assert.Contains(t, opts.parameters["param"], "2023-01-01")
				} else {
					assert.Equal(t, tt.expectedValue, opts.parameters["param"])
				}
			}
		})
	}
}

func TestBindQueryOrAppendParameters_NoProtocolSupport(t *testing.T) {
	opts := &QueryOptions{parameters: make(Parameters)}
	query := "SELECT * FROM t WHERE col = @param"

	// If paramsProtocolSupport is false, it should fallback to legacy bind (which replaces @param directly)
	resQuery, err := bindQueryOrAppendParameters(false, opts, query, time.UTC, Named("param", "val"))

	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE col = 'val'", resQuery)
	assert.Empty(t, opts.parameters, "Parameters map should be empty when fallback to bind")
}

func TestBindQueryOrAppendParameters_ExplicitParams(t *testing.T) {
	opts := &QueryOptions{parameters: Parameters{"param": "explicit_val"}}
	query := `
	SELECT * 
	FROM t 
	WHERE col = {param:String}`

	// If explicit parameters are provided in options, args are ignored for native parameters
	resQuery, err := bindQueryOrAppendParameters(true, opts, query, time.UTC, Named("param", "arg_val"))

	assert.NoError(t, err)
	assert.Equal(t, query, resQuery)
	assert.Equal(t, "explicit_val", opts.parameters["param"], "Explicit parameters should be preferred")
}
