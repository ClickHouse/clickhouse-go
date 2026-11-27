package column

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
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

func TestNullableAppendNilSlices(t *testing.T) {
	ipv4Val := net.ParseIP("192.0.2.1").To4()
	ipv6Val := net.ParseIP("2001:db8::1")
	ipv4Zero := net.IPv4zero.To4()
	ipv6Zero := net.IPv6zero

	tests := []struct {
		name      string
		colType   Type
		rows      []any
		bulk      any
		wantNulls []uint8
		want      []any
	}{
		{
			name:      "Nullable(String) []byte",
			colType:   "Nullable(String)",
			rows:      []any{[]byte("hello"), []byte(nil), []byte{}},
			bulk:      [][]byte{[]byte("hello"), nil, {}},
			wantNulls: []uint8{0, 1, 0},
			want:      []any{"hello", nil, ""},
		},
		{
			name:      "Nullable(String) json.RawMessage",
			colType:   "Nullable(String)",
			rows:      []any{json.RawMessage(`{"a":1}`), json.RawMessage(nil), json.RawMessage{}},
			bulk:      []json.RawMessage{json.RawMessage(`{"a":1}`), nil, {}},
			wantNulls: []uint8{0, 1, 0},
			want:      []any{`{"a":1}`, nil, ""},
		},
		{
			name:      "Nullable(FixedString(4)) []byte",
			colType:   "Nullable(FixedString(4))",
			rows:      []any{[]byte("test"), []byte(nil), []byte{}},
			bulk:      [][]byte{[]byte("test"), nil, {}},
			wantNulls: []uint8{0, 1, 0},
			want:      []any{"test", nil, "\x00\x00\x00\x00"},
		},
		{
			name:      "Nullable(IPv4) net.IP",
			colType:   "Nullable(IPv4)",
			rows:      []any{ipv4Val, net.IP(nil), net.IP{}},
			bulk:      []net.IP{ipv4Val, nil, {}},
			wantNulls: []uint8{0, 1, 0},
			want:      []any{ipv4Val, nil, ipv4Zero},
		},
		{
			name:      "Nullable(IPv6) net.IP",
			colType:   "Nullable(IPv6)",
			rows:      []any{ipv6Val, net.IP(nil), ipv6Zero},
			bulk:      []net.IP{ipv6Val, nil, ipv6Zero},
			wantNulls: []uint8{0, 1, 0},
			want:      []any{ipv6Val, nil, ipv6Zero},
		},
		{
			name:      "Nullable(IPv6) []byte",
			colType:   "Nullable(IPv6)",
			rows:      []any{[]byte(ipv6Val), []byte(nil), []byte(ipv6Zero)},
			bulk:      [][]byte{ipv6Val, nil, ipv6Zero},
			wantNulls: []uint8{0, 1, 0},
			want:      []any{ipv6Val, nil, ipv6Zero},
		},
	}

	assertDecoded := func(t *testing.T, col Interface, want []any) {
		t.Helper()
		var buf proto.Buffer
		col.Encode(&buf)

		decoded, err := col.Type().Column(col.Name(), &ServerContext{})
		require.NoError(t, err)
		require.NoError(t, decoded.Decode(proto.NewReader(bytes.NewReader(buf.Buf)), len(want)))
		require.Equal(t, len(want), decoded.Rows())

		for i, expected := range want {
			got := decoded.Row(i, false)
			switch w := expected.(type) {
			case nil:
				assert.Nil(t, got, "row %d", i)
			case string:
				if assert.NotNil(t, got, "row %d", i) {
					assert.Equal(t, w, *got.(*string), "row %d", i)
				}
			case net.IP:
				if assert.NotNil(t, got, "row %d", i) {
					assert.Equal(t, w, *got.(*net.IP), "row %d", i)
				}
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.name+"/AppendRow", func(t *testing.T) {
			col, err := tt.colType.Column("test", &ServerContext{})
			require.NoError(t, err)
			for _, v := range tt.rows {
				require.NoError(t, col.AppendRow(v))
			}
			assertDecoded(t, col, tt.want)
		})

		t.Run(tt.name+"/Append", func(t *testing.T) {
			col, err := tt.colType.Column("test", &ServerContext{})
			require.NoError(t, err)
			nulls, err := col.Append(tt.bulk)
			require.NoError(t, err)
			assert.Equal(t, tt.wantNulls, nulls)
			assertDecoded(t, col, tt.want)
		})
	}
}

func TestArrayNullableNilSlices(t *testing.T) {
	strVal := "ok"
	fixedVal := "test"
	ipv4Val := net.ParseIP("192.0.2.1").To4()
	ipv6Val := net.ParseIP("2001:db8::1")

	tests := []struct {
		name    string
		colType Type
		input   any
		want    any
	}{
		{
			name:    "Array(Nullable(String)) [][]byte",
			colType: "Array(Nullable(String))",
			input:   [][]byte{[]byte(strVal), nil},
			want:    []*string{&strVal, nil},
		},
		{
			name:    "Array(Nullable(FixedString(4))) [][]byte",
			colType: "Array(Nullable(FixedString(4)))",
			input:   [][]byte{[]byte(fixedVal), nil},
			want:    []*string{&fixedVal, nil},
		},
		{
			name:    "Array(Nullable(IPv4)) []net.IP",
			colType: "Array(Nullable(IPv4))",
			input:   []net.IP{ipv4Val, nil},
			want:    []*net.IP{&ipv4Val, nil},
		},
		{
			name:    "Array(Nullable(IPv6)) []net.IP",
			colType: "Array(Nullable(IPv6))",
			input:   []net.IP{ipv6Val, nil},
			want:    []*net.IP{&ipv6Val, nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, err := tt.colType.Column("test", &ServerContext{})
			require.NoError(t, err)
			require.NoError(t, col.AppendRow(tt.input))

			var buf proto.Buffer
			col.Encode(&buf)

			decoded, err := tt.colType.Column("test", &ServerContext{})
			require.NoError(t, err)
			require.NoError(t, decoded.Decode(proto.NewReader(bytes.NewReader(buf.Buf)), 1))
			assert.Equal(t, tt.want, decoded.Row(0, false))
		})
	}
}
