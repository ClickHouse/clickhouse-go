package column

import (
	"database/sql"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nullScanner records whether Scan(nil) was called and must not be zeroed beforehand.
type nullScanner struct {
	calls int
}

func (s *nullScanner) Scan(src any) error {
	if src == nil {
		s.calls++
	}
	return nil
}

func TestNullableScanRowNullResetsDestination(t *testing.T) {
	ptr := func(v any) any { return &v }

	tests := []struct {
		name  string
		dest  func() any
		check func(t *testing.T, dest any)
	}{
		{"**int64", func() any { v := new(int64); *v = 42; return &v },
			func(t *testing.T, dest any) { assert.Nil(t, *dest.(**int64)) }},
		{"**int", func() any { v := new(int); *v = 42; return &v },
			func(t *testing.T, dest any) { assert.Nil(t, *dest.(**int)) }},
		{"**string", func() any { v := new(string); *v = "stale"; return &v },
			func(t *testing.T, dest any) { assert.Nil(t, *dest.(**string)) }},
		{"**time.Time", func() any { v := new(time.Time); *v = time.Now(); return &v },
			func(t *testing.T, dest any) { assert.Nil(t, *dest.(**time.Time)) }},
		{"**decimal.Decimal", func() any { v := decimal.RequireFromString("123.1"); p := &v; return &p },
			func(t *testing.T, dest any) { assert.Nil(t, *dest.(**decimal.Decimal)) }},
		{"**uuid.UUID", func() any { v := uuid.New(); p := &v; return &p },
			func(t *testing.T, dest any) { assert.Nil(t, *dest.(**uuid.UUID)) }},
		{"**big.Int", func() any { p := big.NewInt(42); return &p },
			func(t *testing.T, dest any) { assert.Nil(t, *dest.(**big.Int)) }},
		{"**net.IP", func() any { v := net.ParseIP("127.0.0.1"); p := &v; return &p },
			func(t *testing.T, dest any) { assert.Nil(t, *dest.(**net.IP)) }},
		{"*any", func() any { return ptr(int64(42)) },
			func(t *testing.T, dest any) { assert.Nil(t, *dest.(*any)) }},
		{"*[]byte", func() any { v := []byte("stale"); return &v },
			func(t *testing.T, dest any) { assert.Nil(t, *dest.(*[]byte)) }},
		{"*sql.NullString", func() any { return &sql.NullString{String: "stale", Valid: true} },
			func(t *testing.T, dest any) { assert.False(t, dest.(*sql.NullString).Valid) }},
		{"*sql.NullInt64", func() any { return &sql.NullInt64{Int64: 42, Valid: true} },
			func(t *testing.T, dest any) { assert.False(t, dest.(*sql.NullInt64).Valid) }},
		{"sql.Scanner is delegated, not reset", func() any { return &nullScanner{calls: 10} },
			func(t *testing.T, dest any) { assert.Equal(t, 11, dest.(*nullScanner).calls) }},
		{"*uuid.UUID keeps its value because UUID.Scan(nil) is a no-op", func() any { v := uuid.MustParse("00000000-0000-0000-0000-000000000001"); return &v },
			func(t *testing.T, dest any) {
				assert.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000001"), *dest.(*uuid.UUID))
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &Nullable{enable: true}
			col.nulls.Append(1)

			dest := tt.dest()
			require.NoError(t, col.ScanRow(dest, 0))
			tt.check(t, dest)
		})
	}
}

// *decimal.Decimal implements sql.Scanner, so its own Scan(nil) error is returned unchanged.
func TestNullableScanRowNullScannerError(t *testing.T) {
	col := &Nullable{enable: true}
	col.nulls.Append(1)

	dest := decimal.RequireFromString("123.1")
	require.Error(t, col.ScanRow(&dest, 0))
}

func TestNullableScanRowNullErrorsOnNonNillableDestination(t *testing.T) {
	var nilPtr *int64
	tests := []struct {
		name string
		dest any
		want string
	}{
		{"*int64", new(int64), "converting NULL to *int64 is unsupported. try using **int64"},
		{"*string", new(string), "converting NULL to *string is unsupported. try using **string"},
		{"*time.Time", new(time.Time), "converting NULL to *time.Time is unsupported. try using **time.Time"},
		{"*bool", new(bool), "converting NULL to *bool is unsupported. try using **bool"},
		{"nil", nil, "destination must be a non-nil pointer"},
		{"non-pointer", int64(42), "destination must be a non-nil pointer"},
		{"nil *int64", nilPtr, "destination must be a non-nil pointer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &Nullable{enable: true}
			col.nulls.Append(1)

			err := col.ScanRow(tt.dest, 0)
			var convErr *ColumnConverterError
			require.ErrorAs(t, err, &convErr)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestNullableScanRowNullErrorKeepsDestination(t *testing.T) {
	col := &Nullable{enable: true}
	col.nulls.Append(1)

	dest := int64(42)
	require.Error(t, col.ScanRow(&dest, 0))
	assert.Equal(t, int64(42), dest)
}
