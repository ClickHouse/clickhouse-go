package timezone

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadFixedOffset verifies that the synthetic "Fixed/UTC±HH:MM:SS" timezone
// names ClickHouse emits for non-IANA fixed offsets resolve to a fixed-offset
// *time.Location instead of failing in time.LoadLocation.
func TestLoadFixedOffset(t *testing.T) {
	tests := []struct {
		name       string
		wantOffset int // seconds east of UTC
	}{
		{"Fixed/UTC+05:30:15", 5*3600 + 30*60 + 15},
		{"Fixed/UTC+05:30:00", 5*3600 + 30*60},
		{"Fixed/UTC-08:30:00", -(8*3600 + 30*60)},
		{"Fixed/UTC+00:00:00", 0},
		{"Fixed/UTC+14:00:00", 14 * 3600},
		{"Fixed/UTC-12:00:00", -12 * 3600},
		// ClickHouse emits fixed offsets up to ±24:00:00 inclusive — pin both extremes.
		{"Fixed/UTC+24:00:00", 24 * 3600},
		{"Fixed/UTC-24:00:00", -24 * 3600},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc, err := Load(tt.name)
			require.NoError(t, err)
			require.NotNil(t, loc)
			// The zone keeps the original synthetic name so DateTime values
			// render with the expected zone label.
			assert.Equal(t, tt.name, loc.String())
			_, offset := time.Date(2023, time.January, 15, 0, 0, 0, 0, loc).Zone()
			assert.Equal(t, tt.wantOffset, offset)
		})
	}
}

// TestLoadFixedOffsetWallClock pins the end-to-end behavior reported in the bug:
// a UTC instant rendered in a Fixed/UTC zone is shifted by exactly the offset,
// matching what the ClickHouse server returns for the same value
// (DateTime('Fixed/UTC+05:30:15') renders 2023-01-15 12:00:00 UTC as
// 2023-01-15 17:30:15).
func TestLoadFixedOffsetWallClock(t *testing.T) {
	loc, err := Load("Fixed/UTC+05:30:15")
	require.NoError(t, err)
	got := time.Unix(1673784000, 0).In(loc).Format("2006-01-02 15:04:05")
	assert.Equal(t, "2023-01-15 17:30:15", got)
}

// TestLoadIANAUnchanged is a contrast case: genuine IANA names must keep
// resolving through time.LoadLocation exactly as before the fix.
func TestLoadIANAUnchanged(t *testing.T) {
	for _, name := range []string{"UTC", "Europe/London", "Asia/Shanghai", "America/New_York"} {
		t.Run(name, func(t *testing.T) {
			loc, err := Load(name)
			require.NoError(t, err)
			want, err := time.LoadLocation(name)
			require.NoError(t, err)
			assert.Equal(t, want.String(), loc.String())
		})
	}
}

// TestLoadUnknownStillErrors is a contrast case: names that are neither IANA nor
// a fixed offset ClickHouse can actually emit must keep returning an error rather
// than being silently coerced to an offset — the seconds-less spelling the server
// never emits, single-digit hours, out-of-range minutes, and offsets beyond the
// ±24:00:00 the server supports (e.g. +25:00:00, which time.Parse also rejects).
func TestLoadUnknownStillErrors(t *testing.T) {
	for _, name := range []string{"Not/AZone", "Fixed/UTC+05:30", "Fixed/UTC+5:30:15", "Fixed/UTC+05:60:00", "Fixed/UTC+25:00:00"} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(name)
			assert.Error(t, err)
		})
	}
}

// TestLoadCaches verifies fixed-offset zones are cached like IANA zones.
func TestLoadCaches(t *testing.T) {
	first, err := Load("Fixed/UTC+05:30:15")
	require.NoError(t, err)
	second, err := Load("Fixed/UTC+05:30:15")
	require.NoError(t, err)
	assert.Same(t, first, second)
}
