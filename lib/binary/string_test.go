package binary

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Str2Bytes backs FixedString encoding (see lib/column/fixed_string.go): a
// value shorter than the column's declared size must be zero-padded to
// exactly that size, or the bytes written to the wire are misaligned.

func TestStr2BytesExactLength(t *testing.T) {
	got := Str2Bytes("hello", 5)
	assert.Equal(t, []byte("hello"), got)
	assert.Len(t, got, 5)
}

func TestStr2BytesLongerThanExpected(t *testing.T) {
	// expectedLen smaller than the string: the string is returned as-is,
	// no truncation happens.
	got := Str2Bytes("hello world", 5)
	assert.Equal(t, []byte("hello world"), got)
}

func TestStr2BytesPadsShorterStringWithZeroBytes(t *testing.T) {
	got := Str2Bytes("hi", 5)
	assert.Len(t, got, 5)
	assert.Equal(t, []byte{'h', 'i', 0, 0, 0}, got)
}

func TestStr2BytesEmptyStringNoPadding(t *testing.T) {
	got := Str2Bytes("", 0)
	assert.Len(t, got, 0)
}

func TestStr2BytesEmptyStringWithPadding(t *testing.T) {
	got := Str2Bytes("", 4)
	assert.Equal(t, []byte{0, 0, 0, 0}, got)
}

func TestStr2BytesMultiByteUTF8(t *testing.T) {
	// FixedString sizes count bytes, not runes; multi-byte characters must
	// come through unmangled and byte-for-byte.
	s := "日本語"
	got := Str2Bytes(s, len(s))
	assert.Equal(t, []byte(s), got)
	assert.Len(t, got, len(s))
}

func TestStr2BytesMultiByteUTF8Padded(t *testing.T) {
	s := "日" // 3 bytes in UTF-8
	got := Str2Bytes(s, 6)
	want := append([]byte(s), 0, 0, 0)
	assert.Equal(t, want, got)
}

func TestStr2BytesAliasesSourceWhenNoPaddingNeeded(t *testing.T) {
	// When expectedLen <= len(str), Str2Bytes hands back a slice that
	// aliases the string's own backing memory (no copy, for speed) rather
	// than a fresh copy. That is intentional, but it means callers must
	// treat the result as read-only and not retain it past the string's
	// lifetime — mutating it corrupts the "immutable" Go string in place.
	// This test pins that contract down so a future change can't flip it
	// silently.
	s := strings.Repeat("a", 8)
	got := Str2Bytes(s, 4)
	got[0] = 'z'
	assert.Equal(t, byte('z'), s[0], "expected the returned slice to alias the source string's memory")
}

func TestStr2BytesLargeInputNoPanic(t *testing.T) {
	s := strings.Repeat("x", 1<<16)
	assert.NotPanics(t, func() {
		got := Str2Bytes(s, len(s))
		assert.Len(t, got, len(s))
	})
}
