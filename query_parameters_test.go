package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestBindQueryDetectsQueryParameter(t *testing.T) {
	options := QueryOptions{}
	query := `SELECT {value:Enum8('enabled' = 1, 'disabled' = 0)}`

	actual, err := bindQueryOrAppendParameters(true, &options, query, time.UTC, Named("value", 1))

	require.NoError(t, err)
	assert.Equal(t, query, actual)
	assert.Equal(t, Parameters{"value": "1"}, options.parameters)
}
