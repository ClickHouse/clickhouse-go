package churl

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnhex_Valid(t *testing.T) {
	tests := []struct {
		input byte
		want  byte
	}{
		{'0', 0},
		{'9', 9},
		{'a', 10},
		{'f', 15},
		{'A', 10},
		{'F', 15},
	}
	for _, tt := range tests {
		got, err := unhex(tt.input)
		require.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}
}

func TestUnhex_Invalid(t *testing.T) {
	invalid := []byte{'G', 'z', '%', ' ', 'x', 0xFF}
	for _, c := range invalid {
		_, err := unhex(c)
		assert.Error(t, err, "expected error for input %q", c)
	}
}

func TestUnescape_InvalidHex(t *testing.T) {
	_, err := unescape("%GG", encodePath)
	require.Error(t, err)
}

func TestUnescape_Truncated(t *testing.T) {
	_, err := unescape("%G", encodePath)
	require.Error(t, err)
}

func TestUnescape_Valid(t *testing.T) {
	got, err := unescape("%48%65%6C%6C%6F", encodePath)
	require.NoError(t, err)
	assert.Equal(t, "Hello", got)
}

func TestParse_InvalidPercentEncoding(t *testing.T) {
	_, err := Parse("http://host:8123/%GG")
	var urlErr *url.Error
	require.ErrorAs(t, err, &urlErr)
}

func TestParse_InvalidPercentEncodingInHost(t *testing.T) {
	_, err := Parse("http://host%GG:8123")
	require.Error(t, err)
}

func TestParse_ValidURL(t *testing.T) {
	u, err := Parse("http://user:pass@host:8123/default?param=value")
	require.NoError(t, err)
	assert.Equal(t, "host:8123", u.Host)
	assert.Equal(t, "/default", u.Path)
	assert.Equal(t, "param=value", u.RawQuery)
}
