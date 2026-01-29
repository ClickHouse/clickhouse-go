package column

import (
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTimeScanType verifies that Time type returns time.Duration from ScanType()
func TestTimeScanType_issue1757(t *testing.T) {
	col := &Time{}

	scanType := col.ScanType()
	expectedType := reflect.TypeOf(time.Duration(0))

	assert.Equal(t, expectedType, scanType,
		"Time.ScanType() should return time.Duration, got %v", scanType)
}

// TestTime64ScanType verifies that Time64 type returns time.Duration from ScanType()
func TestTime64ScanType_issue1757(t *testing.T) {
	col := &Time64{}

	scanType := col.ScanType()
	expectedType := reflect.TypeOf(time.Duration(0))

	assert.Equal(t, expectedType, scanType,
		"Time64.ScanType() should return time.Duration, got %v", scanType)
}

func TestTime64_Append_Store(t *testing.T) {
	// This test ensures that actual value sent to server matches with column's type precision without any additional
	// precision added during time.Duration conversions in between.
	// e.g: for Time64(6) the value sent is int64 with exactly microseconds precision

	cases := []struct {
		name      string
		input     time.Duration
		precision proto.Precision
		expected  int64
	}{
		{
			name:      "second",
			input:     1 * time.Second,
			precision: proto.PrecisionSecond,
			expected:  1,
		},
		{
			name:      "millisecond",
			input:     123 * time.Millisecond,
			precision: proto.PrecisionMilli,
			expected:  123,
		},
		{
			name:      "microsecond",
			input:     123456 * time.Microsecond,
			precision: proto.PrecisionMicro,
			expected:  123456,
		},
		{
			name:      "nanosecond",
			input:     123456789 * time.Nanosecond,
			precision: proto.PrecisionNano,
			expected:  123456789,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := Time64{}
			col.col.Precision = tc.precision

			_, err := col.Append([]time.Duration{
				tc.input,
			})
			require.NoError(t, err)

			v := col.col.Data[0]
			assert.Equal(t, tc.expected, int64(v))
		})
	}
}

func TestTime64_Append(t *testing.T) {
	// Make sure any duration that are getting appened get stored with
	// Column's precision.

	cases := []struct {
		name      string
		input     []time.Duration
		precision proto.Precision
		expected  []time.Duration
	}{
		{
			name: "seconds precision",
			input: []time.Duration{
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456789*time.Nanosecond,
				123456789 * time.Nanosecond,
			},
			precision: proto.Precision(0),
			expected: []time.Duration{
				// should strip everything else except seconds precision
				1 * time.Second,
				1 * time.Second,
				0 * time.Second,
				1 * time.Second,
				0 * time.Second,
				1 * time.Second,
				0 * time.Second,
			},
		},
		{
			name: "milliseconds precision",
			input: []time.Duration{
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456789*time.Nanosecond,
				123456789 * time.Nanosecond,
			},
			precision: proto.Precision(3),
			expected: []time.Duration{
				// should strip everything else except milliseconds precision
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123*time.Millisecond, // microseconds stipped to milliseconds
				123 * time.Millisecond,
				1*time.Second + 123*time.Millisecond, // nanoseconds stipped to milliseconds
				123 * time.Millisecond,
			},
		},
		{
			name: "microseconds precision",
			input: []time.Duration{
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456789*time.Nanosecond,
				123456789 * time.Nanosecond,
			},
			precision: proto.Precision(6),
			expected: []time.Duration{
				// should strip everything else except microseconds precision
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456*time.Microsecond, // nanoseconds stipped to microseconds
				123456 * time.Microsecond,
			},
		},
		{
			name: "nanoseconds precision",
			input: []time.Duration{
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456789*time.Nanosecond,
				123456789 * time.Nanosecond,
			},
			precision: proto.Precision(9),
			expected: []time.Duration{
				// should strip everything else except nanoseconds precision
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456789*time.Nanosecond,
				123456789 * time.Nanosecond,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := Time64{}
			col.col.Precision = tc.precision

			_, err := col.Append(tc.input)
			require.NoError(t, err)

			got := make([]time.Duration, 0)
			for _, v := range col.col.Data {
				got = append(got, v.Duration())
			}
			assert.Equal(t, tc.expected, got)
		})
	}
}
func TestTime64_AppendRow(t *testing.T) {
	// Make sure any duration that are getting appened (via AppendRow api) get stored with
	// Column's precision.

	cases := []struct {
		name      string
		input     []time.Duration
		precision proto.Precision
		expected  []time.Duration
	}{
		{
			name: "seconds precision",
			input: []time.Duration{
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456789*time.Nanosecond,
				123456789 * time.Nanosecond,
			},
			precision: proto.Precision(0),
			expected: []time.Duration{
				// should strip everything else except seconds precision
				1 * time.Second,
				1 * time.Second,
				0 * time.Second,
				1 * time.Second,
				0 * time.Second,
				1 * time.Second,
				0 * time.Second,
			},
		},
		{
			name: "milliseconds precision",
			input: []time.Duration{
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456789*time.Nanosecond,
				123456789 * time.Nanosecond,
			},
			precision: proto.Precision(3),
			expected: []time.Duration{
				// should strip everything else except milliseconds precision
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123*time.Millisecond, // microseconds stipped to milliseconds
				123 * time.Millisecond,
				1*time.Second + 123*time.Millisecond, // nanoseconds stipped to milliseconds
				123 * time.Millisecond,
			},
		},
		{
			name: "microseconds precision",
			input: []time.Duration{
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456789*time.Nanosecond,
				123456789 * time.Nanosecond,
			},
			precision: proto.Precision(6),
			expected: []time.Duration{
				// should strip everything else except microseconds precision
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456*time.Microsecond, // nanoseconds stipped to microseconds
				123456 * time.Microsecond,
			},
		},
		{
			name: "nanoseconds precision",
			input: []time.Duration{
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456789*time.Nanosecond,
				123456789 * time.Nanosecond,
			},
			precision: proto.Precision(9),
			expected: []time.Duration{
				// should strip everything else except nanoseconds precision
				1 * time.Second,
				1*time.Second + 123*time.Millisecond,
				123 * time.Millisecond,
				1*time.Second + 123456*time.Microsecond,
				123456 * time.Microsecond,
				1*time.Second + 123456789*time.Nanosecond,
				123456789 * time.Nanosecond,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := Time64{}
			col.col.Precision = tc.precision

			for _, v := range tc.input {
				err := col.AppendRow(v)
				require.NoError(t, err)
			}

			got := make([]time.Duration, 0)
			for _, v := range col.col.Data {
				got = append(got, v.Duration())
			}
			assert.Equal(t, tc.expected, got)
		})
	}
}
