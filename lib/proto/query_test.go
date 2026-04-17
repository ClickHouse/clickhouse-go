package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeFieldDump(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "plain string",
			input: "hello world",
			want:  `'hello world'`,
		},
		{
			name:  "empty string",
			input: "",
			want:  `''`,
		},
		{
			name:  "single quote",
			input: "it's",
			want:  `'it\'s'`,
		},
		{
			// backslash → 4 backslashes in wire format
			// readQuoted: \\ → \, \\ → \  →  \\  (two backslashes)
			// deserializeTextEscaped: \\ → \
			name:  "backslash",
			input: `a\b`,
			want:  `'a\\\\b'`,
		},
		{
			// tab → \\t in wire format
			// readQuoted: \\ → \, t → t  →  \t  (literal backslash-t)
			// deserializeTextEscaped: \t → tab
			name:  "tab character",
			input: "hello\tworld",
			want:  `'hello\\tworld'`,
		},
		{
			// same double-encoding for newline
			name:  "newline character",
			input: "hello\nworld",
			want:  `'hello\\nworld'`,
		},
		{
			name:  "carriage return",
			input: "hello\rworld",
			want:  `'hello\\rworld'`,
		},
		{
			name:  "nul byte",
			input: "hello\x00world",
			want:  `'hello\\0world'`,
		},
		{
			// literal backslash-t (not a tab): backslash → \\\\, t stays
			name:  "backslash followed by t (not a tab)",
			input: `hello\tworld`,
			want:  `'hello\\\\tworld'`,
		},
		{
			// literal backslash then quote: backslash → \\\\, quote → \'
			name:  "backslash followed by single quote",
			input: `a\'b`,
			want:  `'a\\\\\'b'`,
		},
		{
			// tab:\there\nnewline\backslash'quote
			// tab → \\t, \n → \\n, \ → \\\\, ' → \'
			name:  "mixed special characters",
			input: "tab:\there\nnewline\\backslash'quote",
			want:  `'tab:\\there\\nnewline\\\\backslash\'quote'`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeFieldDump(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("unsupported type", func(t *testing.T) {
		_, err := encodeFieldDump(42)
		require.Error(t, err)
	})
}
