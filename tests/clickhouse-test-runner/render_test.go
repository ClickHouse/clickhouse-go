package main

import (
	"math"
	"math/big"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatFloat(t *testing.T) {
	// Variables, so that the compiler does not fold 0.1 + 0.2 exactly.
	a, b := 0.1, 0.2
	// The expected text is what ClickHouse 26.10 writes for FORMAT TSV.
	tests := []struct {
		in   float64
		bits int
		want string
	}{
		{1e15, 64, "1000000000000000"},
		{1e20, 64, "100000000000000000000"},
		{1e21, 64, "1e21"},
		{1.2345e21, 64, "1.2345e21"},
		{1.2345e20, 64, "123450000000000000000"},
		{0.0001, 64, "0.0001"},
		{1e-6, 64, "0.000001"},
		{1.5e-6, 64, "0.0000015"},
		{1e-7, 64, "1e-7"},
		{1.5e-7, 64, "1.5e-7"},
		{1.5e300, 64, "1.5e300"},
		{a + b, 64, "0.30000000000000004"},
		{5e-324, 64, "5e-324"},
		{-12345678.9, 64, "-12345678.9"},
		{100, 64, "100"},
		{math.NaN(), 64, "nan"},
		{math.Inf(1), 64, "inf"},
		{math.Inf(-1), 64, "-inf"},
		{math.Copysign(0, -1), 64, "-0"},
		{0, 64, "0"},
		{float64(float32(0.1)), 32, "0.1"},
		{float64(float32(3.4028235e38)), 32, "3.4028235e38"},
		{float64(float32(16777216)), 32, "16777216"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, formatFloat(tt.in, tt.bits), "formatFloat(%v, %d)", tt.in, tt.bits)
	}
}

func TestFormatIPv6(t *testing.T) {
	for in, want := range map[string]string{
		"::ffff:1.2.3.4":  "::ffff:1.2.3.4",
		"::1.2.3.4":       "::1.2.3.4",
		"::":              "::",
		"::1":             "::1",
		"2001:db8::1":     "2001:db8::1",
		"1::":             "1::",
		"2001:db8:0:1::1": "2001:db8:0:1::1",
		"1:0:0:2:0:0:0:3": "1:0:0:2::3",
		"1:2:3:4:5:6:7:8": "1:2:3:4:5:6:7:8",
	} {
		assert.Equal(t, want, formatIPv6(net.ParseIP(in).To16()), in)
	}
}

func TestParseCHType(t *testing.T) {
	for _, in := range []string{
		"UInt8",
		"Nullable(String)",
		"Array(Nullable(DateTime64(3, 'UTC')))",
		"Map(String, Array(Tuple(UInt8, String)))",
		"Tuple(a Int32, b String)",
		"Enum8('a' = 1, 'b,c' = 2)",
		"Decimal(9, 2)",
		"LowCardinality(Nullable(String))",
		"SimpleAggregateFunction(sum, UInt64)",
		"AggregateFunction(quantiles(0.5, 0.9), UInt64)",
	} {
		typ, err := parseCHType(in)
		require.NoError(t, err, in)
		assert.Equal(t, in, typ.String())
	}

	typ, err := parseCHType("Tuple(`a b` Int32, c Array(String))")
	require.NoError(t, err)
	require.Len(t, typ.Args, 2)
	assert.Equal(t, "a b", typ.Args[0].Name)
	assert.Equal(t, "c", typ.Args[1].Name)
	assert.Equal(t, "Array", typ.Args[1].Type.Base)

	_, err = parseCHType("Array(String")
	assert.Error(t, err)
}

func render(t *testing.T, typeName string, v any) string {
	t.Helper()
	typ, err := parseCHType(typeName)
	require.NoError(t, err)
	var sb strings.Builder
	require.NoError(t, renderValue(&sb, typ, reflect.ValueOf(v), false))
	return sb.String()
}

func TestRenderValue(t *testing.T) {
	str := func(s string) *string { return &s }
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)

	assert.Equal(t, `a\tb\\c\nd\'e\0\b\f`+"\x01", render(t, "String", "a\tb\\c\nd'e\x00\b\f\x01"))
	assert.Equal(t, `\N`, render(t, "Nullable(String)", (*string)(nil)))
	assert.Equal(t, "x", render(t, "Nullable(String)", str("x")))
	assert.Equal(t, `['a\'b',NULL]`, render(t, "Array(Nullable(String))", []*string{str("a'b"), nil}))
	assert.Equal(t, "(1,'x',NULL)", render(t, "Tuple(UInt8, String, Nullable(UInt8))", []any{uint8(1), "x", nil}))
	assert.Equal(t, "(1,'x')", render(t, "Tuple(a UInt8, b String)", map[string]any{"b": "x", "a": uint8(1)}))
	assert.Equal(t, "{'k':1}", render(t, "Map(String, UInt8)", map[string]uint8{"k": 1}))
	assert.Equal(t, "-5", render(t, "Int128", big.NewInt(-5)))
	assert.Equal(t, "1.5", render(t, "Decimal(9, 2)", decimal.New(150, -2)))
	assert.Equal(t, "0", render(t, "Decimal(9, 2)", decimal.New(0, -2)))
	assert.Equal(t, "2020-01-01 00:00:00.120",
		render(t, "DateTime64(3)", time.Date(2020, 1, 1, 0, 0, 0, 120e6, time.UTC)))
	assert.Equal(t, "2020-01-01 09:00:00",
		render(t, "DateTime('Asia/Tokyo')", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).In(tokyo)))
	assert.Equal(t, "['2020-01-02']", render(t, "Array(Date)", []time.Time{time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)}))
	assert.Equal(t, "-01:02:03", render(t, "Time", -(time.Hour+2*time.Minute+3*time.Second)))
	assert.Equal(t, "100:00:00.500", render(t, "Time64(3)", 100*time.Hour+500*time.Millisecond))
	// The server writes a value above 999:59:59 as 999:59:59.
	assert.Equal(t, "999:59:59.00", render(t, "Time64(2)", 2160*time.Hour+12*time.Second))
	assert.Equal(t, "-999:59:59", render(t, "Time", -2000*time.Hour))
	assert.Equal(t, "61f0c404-5cb3-11e7-907b-a6006ad3dba0",
		render(t, "UUID", uuid.MustParse("61f0c404-5cb3-11e7-907b-a6006ad3dba0")))
	assert.Equal(t, "1.2.3.4", render(t, "IPv4", net.ParseIP("1.2.3.4")))
	assert.Equal(t, "['::ffff:1.2.3.4']", render(t, "Array(IPv6)", []net.IP{net.ParseIP("::ffff:1.2.3.4")}))
	assert.Equal(t, "true", render(t, "Bool", true))
	assert.Equal(t, "[nan,inf]", render(t, "Array(Float64)", []float64{math.NaN(), math.Inf(1)}))
	assert.Equal(t, "3", render(t, "IntervalDay", "3 Days"))
	assert.Equal(t, "a", render(t, "LowCardinality(String)", "a"))
	assert.Equal(t, "7", render(t, "SimpleAggregateFunction(sum, UInt64)", uint64(7)))
}

func TestRenderValueUnsupported(t *testing.T) {
	typ, err := parseCHType("Map(String, Map(String, UInt8))")
	require.NoError(t, err)
	var sb strings.Builder
	// A nested map with more than one entry has no stable order.
	err = renderValue(&sb, typ.typeArg(1), reflect.ValueOf(map[string]uint8{"a": 1, "b": 2}), true)
	var unsupported *errUnsupported
	assert.ErrorAs(t, err, &unsupported)

	typ, err = parseCHType("Dynamic")
	require.NoError(t, err)
	err = renderValue(&sb, typ, reflect.ValueOf(1), false)
	assert.ErrorAs(t, err, &unsupported)
}

func TestSplitOutputFormat(t *testing.T) {
	tests := []struct {
		in, sql, format string
		supported       bool
	}{
		{"SELECT 1", "SELECT 1", "TabSeparated", true},
		{"SELECT 1 FORMAT TSV", "SELECT 1 ", "TSV", true},
		{"SELECT 1 FORMAT TSVWithNames SETTINGS max_threads = 1", "SELECT 1  SETTINGS max_threads = 1", "TSVWithNames", true},
		{"SELECT 1 FORMAT JSONEachRow", "SELECT 1 ", "JSONEachRow", false},
		{"SELECT 'FORMAT JSON'", "SELECT 'FORMAT JSON'", "TabSeparated", true},
		{"SELECT * FROM (SELECT 1 FORMAT JSON)", "SELECT * FROM (SELECT 1 FORMAT JSON)", "TabSeparated", true},
		{"SELECT t.format FROM t", "SELECT t.format FROM t", "TabSeparated", true},
		{"SELECT 1 FORMAT null", "SELECT 1 ", "null", true},
		{"SELECT 1 FORMAT TSVRaw", "SELECT 1 ", "TSVRaw", true},
		{"SELECT 1 FORMAT `Null`;", "SELECT 1 ;", "Null", true},
		// FORMAT as an identifier, not the clause.
		{"SELECT format FROM t", "SELECT format FROM t", "TabSeparated", true},
		{"SELECT x AS format FROM t WHERE format = 1", "SELECT x AS format FROM t WHERE format = 1", "TabSeparated", true},
		{"SELECT 1 AS format FROM numbers(1)", "SELECT 1 AS format FROM numbers(1)", "TabSeparated", true},
		{"SELECT format FROM (SELECT 1 AS format)", "SELECT format FROM (SELECT 1 AS format)", "TabSeparated", true},
		{"SELECT 1 AS format", "SELECT 1 AS format", "TabSeparated", true},
		{"SELECT 1 AS format ORDER BY format DESC", "SELECT 1 AS format ORDER BY format DESC", "TabSeparated", true},
		{"SELECT 1 AS format ORDER BY format ASC", "SELECT 1 AS format ORDER BY format ASC", "TabSeparated", true},
		{"SELECT 1 AS format ORDER BY format DESC FORMAT TSV", "SELECT 1 AS format ORDER BY format DESC ", "TSV", true},
		// An implicit alias after a column named `format`.
		{"SELECT 1 AS format, format x", "SELECT 1 AS format, format x", "TabSeparated", true},
		{"SELECT 1 AS format, format x FORMAT TSV", "SELECT 1 AS format, format x ", "TSV", true},
		{"SELECT number + format y FROM (SELECT 1 AS format, number FROM numbers(1))", "SELECT number + format y FROM (SELECT 1 AS format, number FROM numbers(1))", "TabSeparated", true},
		{"SELECT (1) FORMAT TSV", "SELECT (1) ", "TSV", true},
		// A FORMAT clause after a select-star or a comment.
		{"SELECT * FORMAT TSV", "SELECT * ", "TSV", true},
		{"FROM t SELECT * FORMAT TSV", "FROM t SELECT * ", "TSV", true},
		{"SELECT t.* FROM t FORMAT TSV", "SELECT t.* FROM t ", "TSV", true},
		{"SELECT 1 /* c */ FORMAT TSV", "SELECT 1 /* c */ ", "TSV", true},
		{"SELECT 1 -- c,\nFORMAT TSV", "SELECT 1 -- c,\n", "TSV", true},
		{"SELECT 'a,' FORMAT TSV", "SELECT 'a,' ", "TSV", true},
		// A multiplication by a column named `format`.
		{"SELECT 2 * format x FROM (SELECT 1 AS format)", "SELECT 2 * format x FROM (SELECT 1 AS format)", "TabSeparated", true},
		{"SELECT x FROM t GROUP BY ALL FORMAT TSV", "SELECT x FROM t GROUP BY ALL ", "TSV", true},
		{"SELECT x FROM t ORDER BY ALL DESC FORMAT TSV", "SELECT x FROM t ORDER BY ALL DESC ", "TSV", true},
		{"SELECT 'a' FORMAT TSV", "SELECT 'a' ", "TSV", true},
		{"SELECT 1 FORMAT TSV -- c\n;", "SELECT 1  -- c\n;", "TSV", true},
	}
	for _, tt := range tests {
		sql, f := splitOutputFormat(tt.in)
		assert.Equal(t, tt.sql, sql, tt.in)
		assert.Equal(t, tt.format, f.Name, tt.in)
		assert.Equal(t, tt.supported, f.Supported, tt.in)
	}
}
