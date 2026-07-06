package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEncodeFieldDump checks the quoted Field dump used to send query
// parameters over the native protocol. Both single quotes and backslashes
// must be escaped — the server unescapes the dump when reading it back, so
// an unescaped backslash corrupts the value (#1898).
func TestEncodeFieldDump(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  string
	}{
		{"plain", "abc", `'abc'`},
		{"single quote", "a'b", `'a\'b'`},
		{"backslash", `a\b`, `'a\\b'`},
		{"backslash before quote", `a\'b`, `'a\\\'b'`},
		{"map text format", `{'a\'b\\c':1}`, `'{\'a\\\'b\\\\c\':1}'`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeFieldDump(tc.value)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("unsupported type", func(t *testing.T) {
		_, err := encodeFieldDump(42)
		require.Error(t, err)
	})
}
