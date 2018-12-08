package lz4

import (
	"bytes"
	"io/ioutil"
	"testing"
)

var testfile, _ = ioutil.ReadFile("testdata/pg1661.txt")

func roundtrip(t *testing.T, input []byte) {
	output := make([]byte, CompressBound(len(input)))
	compressedSize, err := Encode(output, input)
	if err != nil {
		t.Errorf("got error during compression: %s", err)
	}

	var newInput = make([]byte, len(input))

	decompressedSize, err := Decode(newInput, output[:compressedSize])

	if decompressedSize != len(newInput) {
		t.Errorf("decompressed size not match")
	}

	if err != nil {
		t.Errorf("got error during decompress: %s", err)
	}
	if !bytes.Equal(newInput, input) {
		t.Error("roundtrip failed", len(input))
	}
}

func TestEmpty(t *testing.T) {
	roundtrip(t, nil)
}

func TestLengths(t *testing.T) {

	for i := 0; i < 1024; i++ {
		roundtrip(t, testfile[:i])
	}

	for i := 1024; i < len(testfile); i += 1024 * 4 {
		roundtrip(t, testfile[:i])
	}
}

func TestWords(t *testing.T) {
	roundtrip(t, testfile)
}

func BenchmarkLZ4Encode(b *testing.B) {
	output := make([]byte, CompressBound(len(testfile)))
	for i := 0; i < b.N; i++ {
		Encode(output, testfile[:1024])
	}
}

func BenchmarkLZ4Decode(b *testing.B) {
	output := make([]byte, CompressBound(len(testfile)))
	Encode(output, testfile)
	inputNew := make([]byte, len(testfile))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Decode(inputNew, output)
	}
}
