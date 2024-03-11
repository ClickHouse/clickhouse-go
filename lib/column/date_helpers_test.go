package column

import (
	"testing"
	"time"
)

func TestDateOverflow(t *testing.T) {
	t.Parallel()
	zeroTime := time.Time{}
	tests := []struct {
		v    time.Time
		name string
		err  bool
	}{
		{
			name: "valid date",
			v:    time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			err:  false,
		},
		{
			name: "before min date",
			v:    minDate.Add(-time.Second),
			err:  true,
		},
		{
			name: "after max date",
			v:    maxDate.Add(time.Second),
			err:  true,
		},
		{
			name: "zero value date",
			v:    zeroTime,
			err:  false,
		},
		{
			name: "non-zero value equal to zero date",
			v:    time.UnixMilli(zeroTime.UnixMilli()),
			err:  false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := dateOverflow(minDateTime, maxDateTime, test.v, defaultDateFormatNoZone)
			if (err != nil) != test.err {
				t.Errorf("expected error: %v, got: %v", test.err, err)
			}
		})
	}
}
