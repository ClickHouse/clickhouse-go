package binary

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// Str2Bytes backs FixedString encoding (see lib/column/fixed_string.go): a
// value shorter than the column's declared size must be zero-padded to
// exactly that size, or the bytes written to the wire are misaligned.
func TestStr2Bytes(t *testing.T) {
	cases := []struct {
		name        string
		str         string
		expectedLen int
		want        []byte
	}{
		{"exact length", "hello", 5, []byte("hello")},
		{"longer than expected returns the string as-is, no truncation", "hello world", 5, []byte("hello world")},
		{"shorter string is zero-padded", "hi", 5, []byte{'h', 'i', 0, 0, 0}},
		{"empty string, no padding", "", 0, []byte{}},
		{"empty string, zero-padded", "", 4, []byte{0, 0, 0, 0}},
		// FixedString sizes count bytes, not runes; multi-byte characters
		// must come through unmangled and byte-for-byte.
		{"multi-byte UTF-8, exact length", "日本語", len("日本語"), []byte("日本語")},
		{"multi-byte UTF-8, zero-padded", "日", 6, append([]byte("日"), 0, 0, 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Str2Bytes(tc.str, tc.expectedLen)
			// compared as strings: Str2Bytes("", 0) may be nil, not empty
			assert.Equal(t, string(tc.want), string(got))
			assert.Len(t, got, len(tc.want))
		})
	}
}

func TestStr2BytesAliasesSourceWhenNoPaddingNeeded(t *testing.T) {
	// When expectedLen <= len(str), Str2Bytes hands back a slice that
	// aliases the string's own backing memory (no copy, for speed) rather
	// than a fresh copy. That is intentional, but it means callers must
	// treat the result as read-only and not retain it past the string's
	// lifetime — mutating it corrupts the "immutable" Go string in place.
	// This test pins that contract down so a future change can't flip it
	// silently.
	//
	// Str2Bytes has an arch-specific implementation: on amd64/arm64 it uses
	// an unsafe zero-copy conversion and the returned slice aliases the
	// source string, while on other architectures it uses []byte(str),
	// which copies.
	s := strings.Repeat("a", 8)
	got := Str2Bytes(s, 4)
	// pointer identity, not a write: modifying the bytes would be undefined
	aliases := unsafe.SliceData(got) == unsafe.StringData(s)
	if runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64" {
		assert.True(t, aliases, "expected the returned slice to alias the source string's memory on this architecture")
		return
	}
	assert.False(t, aliases, "expected the returned slice to be a copy on this architecture")
}

func TestStr2BytesLargeInputNoPanic(t *testing.T) {
	s := strings.Repeat("x", 1<<16)
	assert.NotPanics(t, func() {
		got := Str2Bytes(s, len(s))
		assert.Len(t, got, len(s))
	})
}
