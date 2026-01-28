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
	// col := &Time64{
	// 	col: proto.ColTime64{
	// 		Precision: proto.PrecisionMicro,
	// 	},
	// }
	//
	// col.Append([]time.Duration{
	// 	10 * time.Second,
	// 	10 * time.Millisecond,
	// 	10 * time.Microsecond,
	// 	123456789 * time.Nanosecond,
	// })
	//
	// assert.Equal(t, col.col.Data[0].Duration(), 10*time.Second)
	// assert.Equal(t, col.col.Data[1].Duration(), 10*time.Millisecond)
	// assert.Equal(t, col.col.Data[2].Duration(), 10*time.Microsecond)
	// assert.Equal(t, col.col.Data[3].Duration(), 123456*time.Nanosecond)
}
