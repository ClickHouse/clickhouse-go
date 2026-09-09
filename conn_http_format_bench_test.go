package clickhouse

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// benchScanPayload returns size bytes of payload. The "clean" flavor contains
// no exception-frame bytes at all; the "markerish" flavor carries a full bare
// marker (without CRLF framing) in every 16 bytes, forcing the scanner through
// the edge cases of near-miss and split-boundary holdback paths on every fill.
func benchScanPayload(flavor string, size int) []byte {
	var unit []byte
	switch flavor {
	case "clean":
		unit = []byte("0123456789abcdef")
	case "markerish":
		unit = []byte("x__exception__xx")
	default:
		panic("unknown payload flavor: " + flavor)
	}
	return bytes.Repeat(unit, size/len(unit))
}

// BenchmarkExceptionScanReader measures the pass-through overhead of the
// mid-stream exception scan on a clean 1 MiB response drained with various
// caller read sizes.
// Small read sizes are the regression trap: the scan cost
// per Read must stay bounded by the holdback window, not by the amount of
// data pending
func BenchmarkExceptionScanReader(b *testing.B) {
	const payloadSize = 1 << 20
	for _, flavor := range []string{"clean", "markerish"} {
		payload := benchScanPayload(flavor, payloadSize)
		for _, readSize := range []int{16, 512, 4096, 64 << 10} {
			b.Run(fmt.Sprintf("%s/readSize=%d", flavor, readSize), func(b *testing.B) {
				p := make([]byte, readSize)
				b.SetBytes(payloadSize)
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					r := newExceptionScanReader(bytes.NewReader(payload), "tag123")
					for {
						if _, err := r.Read(p); err != nil {
							if errors.Is(err, io.EOF) {
								break
							}
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

// TestExceptionScanReaderScanOffsetBounded pins the property that keeps Read
// linear.
// After scan sees a fill with no complete frame, everything except
// the split-frame holdback window is marked decided, so safeLen re-examines
// at most len(exceptionFrame)-1 bytes per Read instead of rescanning the
// whole pending buffer on every call.
func TestExceptionScanReaderScanOffsetBounded(t *testing.T) {
	r := newExceptionScanReader(bytes.NewReader(nil), "tag123")
	r.pending = bytes.Repeat([]byte("x"), 8<<10)
	r.scan()
	require.Equal(t, len(r.pending)-(len(exceptionFrame)-1), r.scanOffset,
		"clean fill: scanOffset must advance to the holdback boundary")

	// An undecided candidate (complete frame, tag bytes still in flight) must
	// stay at or right of scanOffset so safeLen keeps holding it back.
	r = newExceptionScanReader(bytes.NewReader(nil), "tag123")
	r.pending = append(bytes.Repeat([]byte("x"), 100), exceptionFrame...)
	r.scan()
	require.Equal(t, r.scanOffset, 100,
		"undecided candidate must remain inside the scanned window")
}
