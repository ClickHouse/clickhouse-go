package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeFieldDump(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "plain string unchanged",
			input:    "hello",
			expected: "'hello'",
		},
		{
			name:     "tab is TSV-escaped",
			input:    "hello\tworld",
			expected: `'hello\tworld'`,
		},
		{
			name:     "newline is TSV-escaped",
			input:    "hello\nworld",
			expected: `'hello\nworld'`,
		},
		{
			name:     "carriage return is TSV-escaped",
			input:    "hello\rworld",
			expected: `'hello\rworld'`,
		},
		{
			name:     "backslash is doubled",
			input:    `hello\world`,
			expected: `'hello\\world'`,
		},
		{
			name:     "single quote is escaped",
			input:    "it's",
			expected: `'it\'s'`,
		},
		{
			name:     "null byte is escaped",
			input:    "hello\x00world",
			expected: `'hello\0world'`,
		},
		{
			name:     "multiple special chars",
			input:    "a\tb\nc",
			expected: `'a\tb\nc'`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: "''",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeFieldDump(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}

	t.Run("unsupported type returns error", func(t *testing.T) {
		_, err := encodeFieldDump(42)
		assert.Error(t, err)
	})
}
