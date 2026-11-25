package main

import (
	"fmt"
	"math"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"github.com/shopspring/decimal"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
)

// The renderer writes values that clickhouse-go scanned into Go types as the
// text that the server writes for the `TabSeparated` format. The upstream
// `.reference` files hold the server's text, so a test passes only when the
// driver decoded every value correctly.
//
// The rendering is type-directed: the Go value alone is not sufficient (for
// example, the precision of a `DateTime64` is in the type only).
//
// ClickHouse uses two text forms:
//   - top level (`serializeTextEscaped`): strings are escaped, not quoted, and
//     NULL is `\N`;
//   - nested in `Array`, `Tuple` and `Map` (`serializeTextQuoted`): strings are
//     quoted, and NULL is `NULL`.

// errUnsupported marks a type that the renderer does not support. The
// statement fails, so the test cannot pass by accident.
type errUnsupported struct{ typ string }

func (e *errUnsupported) Error() string {
	return "clickhouse-test-runner: no TabSeparated renderer for type " + e.typ
}

// orderedMap receives a top-level `Map` column in server order. A Go map has
// no order, and the `.reference` files depend on it.
type orderedMap struct {
	keys   []any
	values []any
}

func (m *orderedMap) Put(key, value any) {
	m.keys = append(m.keys, key)
	m.values = append(m.values, value)
}

func (m *orderedMap) Iterator() column.MapIterator { return &orderedMapIterator{m: m, i: -1} }

type orderedMapIterator struct {
	m *orderedMap
	i int
}

func (it *orderedMapIterator) Next() bool { it.i++; return it.i < len(it.m.keys) }
func (it *orderedMapIterator) Key() any   { return it.m.keys[it.i] }
func (it *orderedMapIterator) Value() any { return it.m.values[it.i] }

var _ column.IterableOrderedMap = (*orderedMap)(nil)

// columnRenderer renders the values of one result column.
type columnRenderer struct {
	typ      *chType
	scanType reflect.Type
}

func newColumnRenderer(typeName string, scanType reflect.Type) (*columnRenderer, error) {
	t, err := parseCHType(typeName)
	if err != nil {
		return nil, err
	}
	return &columnRenderer{typ: t, scanType: scanType}, nil
}

// dest returns a new scan destination for this column.
func (c *columnRenderer) dest() any {
	if unwrapLowCardinality(c.typ).Base == "Map" {
		return &orderedMap{}
	}
	return reflect.New(c.scanType).Interface()
}

// render writes the scanned value at dest (as returned by dest()). With raw
// (the `TSVRaw` formats), a top-level string is written without escapes.
func (c *columnRenderer) render(sb *strings.Builder, dest any, raw bool) error {
	if m, ok := dest.(*orderedMap); ok {
		return renderOrderedMap(sb, unwrapLowCardinality(c.typ), m)
	}
	v := reflect.ValueOf(dest).Elem()
	if raw {
		if dv, ok := deref(v); ok && dv.Kind() == reflect.String && isStringType(c.typ) {
			sb.WriteString(dv.String())
			return nil
		}
	}
	return renderValue(sb, c.typ, v, false)
}

func isStringType(t *chType) bool {
	for (t.Base == "Nullable" || t.Base == "LowCardinality") && t.typeArg(0) != nil {
		t = t.typeArg(0)
	}
	switch t.Base {
	case "String", "FixedString", "Enum8", "Enum16":
		return true
	}
	return false
}

func unwrapLowCardinality(t *chType) *chType {
	for t.Base == "LowCardinality" && t.typeArg(0) != nil {
		t = t.typeArg(0)
	}
	return t
}

func renderOrderedMap(sb *strings.Builder, t *chType, m *orderedMap) error {
	kt, vt := t.typeArg(0), t.typeArg(1)
	if kt == nil || vt == nil {
		return &errUnsupported{typ: t.String()}
	}
	sb.WriteByte('{')
	for i := range m.keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		if err := renderValue(sb, kt, reflect.ValueOf(m.keys[i]), true); err != nil {
			return err
		}
		sb.WriteByte(':')
		if err := renderValue(sb, vt, reflect.ValueOf(m.values[i]), true); err != nil {
			return err
		}
	}
	sb.WriteByte('}')
	return nil
}

// deref follows pointers and interfaces. It returns ok=false for a nil value,
// which is how the driver gives a NULL.
func deref(v reflect.Value) (reflect.Value, bool) {
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return v, false
		}
		// A *big.Int is a value, not a nullable wrapper.
		if v.Type() == reflect.TypeOf(&big.Int{}) {
			return v, true
		}
		v = v.Elem()
	}
	return v, v.IsValid()
}

func renderValue(sb *strings.Builder, t *chType, v reflect.Value, nested bool) error {
	switch t.Base {
	case "Nullable", "LowCardinality":
		inner := t.typeArg(0)
		if inner == nil {
			return &errUnsupported{typ: t.String()}
		}
		return renderValue(sb, inner, v, nested)
	case "SimpleAggregateFunction":
		inner := t.typeArg(len(t.Args) - 1)
		if inner == nil {
			return &errUnsupported{typ: t.String()}
		}
		return renderValue(sb, inner, v, nested)
	}

	v, ok := deref(v)
	if !ok {
		if nested {
			sb.WriteString("NULL")
		} else {
			sb.WriteString(`\N`)
		}
		return nil
	}

	switch t.Base {
	case "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
		"Int128", "Int256", "UInt128", "UInt256":
		return renderInteger(sb, t, v)
	case "Float32", "BFloat16":
		f, err := floatOf(t, v)
		if err != nil {
			return err
		}
		sb.WriteString(formatFloat(f, 32))
	case "Float64":
		f, err := floatOf(t, v)
		if err != nil {
			return err
		}
		sb.WriteString(formatFloat(f, 64))
	case "Bool":
		if v.Kind() != reflect.Bool {
			return typeMismatch(t, v)
		}
		sb.WriteString(strconv.FormatBool(v.Bool()))
	case "String", "FixedString", "Enum8", "Enum16":
		if v.Kind() != reflect.String {
			return typeMismatch(t, v)
		}
		writeString(sb, v.String(), nested)
	case "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256":
		d, ok := v.Interface().(decimal.Decimal)
		if !ok {
			return typeMismatch(t, v)
		}
		sb.WriteString(d.String())
	case "Date", "Date32":
		tm, ok := v.Interface().(time.Time)
		if !ok {
			return typeMismatch(t, v)
		}
		writeQuotedIf(sb, tm.Format("2006-01-02"), nested)
	case "DateTime":
		tm, ok := v.Interface().(time.Time)
		if !ok {
			return typeMismatch(t, v)
		}
		writeQuotedIf(sb, tm.Format("2006-01-02 15:04:05"), nested)
	case "DateTime64":
		tm, ok := v.Interface().(time.Time)
		if !ok {
			return typeMismatch(t, v)
		}
		precision, err := strconv.Atoi(t.literalArg(0))
		if err != nil || precision < 0 || precision > 9 {
			return &errUnsupported{typ: t.String()}
		}
		writeQuotedIf(sb, formatDateTime64(tm, precision), nested)
	case "Time", "Time64":
		d, ok := v.Interface().(time.Duration)
		if !ok {
			return typeMismatch(t, v)
		}
		precision := 0
		if t.Base == "Time64" {
			p, err := strconv.Atoi(t.literalArg(0))
			if err != nil || p < 0 || p > 9 {
				return &errUnsupported{typ: t.String()}
			}
			precision = p
		}
		writeQuotedIf(sb, formatTime(d, precision), nested)
	case "UUID":
		u, ok := v.Interface().(uuid.UUID)
		if !ok {
			return typeMismatch(t, v)
		}
		writeQuotedIf(sb, u.String(), nested)
	case "IPv4":
		ip, ok := v.Interface().(net.IP)
		if !ok || ip.To4() == nil {
			return typeMismatch(t, v)
		}
		writeQuotedIf(sb, ip.To4().String(), nested)
	case "IPv6":
		ip, ok := v.Interface().(net.IP)
		if !ok || len(ip.To16()) != net.IPv6len {
			return typeMismatch(t, v)
		}
		writeQuotedIf(sb, formatIPv6(ip.To16()), nested)
	case "Nothing":
		// A `Nothing` column has no values; a value here is a NULL.
		if nested {
			sb.WriteString("NULL")
		} else {
			sb.WriteString(`\N`)
		}
	case "Array":
		inner := t.typeArg(0)
		if inner == nil || (v.Kind() != reflect.Slice && v.Kind() != reflect.Array) {
			return typeMismatch(t, v)
		}
		return renderSequence(sb, inner, v, '[', ']')
	case "Tuple":
		return renderTuple(sb, t, v)
	case "Map":
		return renderGoMap(sb, t, v)
	case "Point":
		p, ok := v.Interface().(orb.Point)
		if !ok {
			return typeMismatch(t, v)
		}
		renderPoint(sb, p)
	case "Ring", "LineString":
		return renderSequence(sb, &chType{Base: "Point"}, v, '[', ']')
	case "Polygon", "MultiLineString":
		return renderSequence(sb, &chType{Base: "Ring"}, v, '[', ']')
	case "MultiPolygon":
		return renderSequence(sb, &chType{Base: "Polygon"}, v, '[', ']')
	default:
		if strings.HasPrefix(t.Base, "Interval") {
			// The driver scans an interval as text, such as "3 Days".
			if v.Kind() != reflect.String {
				return typeMismatch(t, v)
			}
			n, _, _ := strings.Cut(v.String(), " ")
			sb.WriteString(n)
			return nil
		}
		return &errUnsupported{typ: t.String()}
	}
	return nil
}

func typeMismatch(t *chType, v reflect.Value) error {
	return fmt.Errorf("clickhouse-test-runner: unexpected Go value %s for type %s", v.Type(), t)
}

func renderInteger(sb *strings.Builder, t *chType, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		sb.WriteString(strconv.FormatInt(v.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		sb.WriteString(strconv.FormatUint(v.Uint(), 10))
	default:
		switch n := v.Interface().(type) {
		case *big.Int:
			sb.WriteString(n.String())
		case big.Int:
			sb.WriteString(n.String())
		default:
			return typeMismatch(t, v)
		}
	}
	return nil
}

func floatOf(t *chType, v reflect.Value) (float64, error) {
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	}
	return 0, typeMismatch(t, v)
}

func renderSequence(sb *strings.Builder, inner *chType, v reflect.Value, open, closing byte) error {
	sb.WriteByte(open)
	for i := 0; i < v.Len(); i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		if err := renderValue(sb, inner, v.Index(i), true); err != nil {
			return err
		}
	}
	sb.WriteByte(closing)
	return nil
}

func renderTuple(sb *strings.Builder, t *chType, v reflect.Value) error {
	sb.WriteByte('(')
	defer sb.WriteByte(')')
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		if v.Len() != len(t.Args) {
			return typeMismatch(t, v)
		}
		for i, a := range t.Args {
			if i > 0 {
				sb.WriteByte(',')
			}
			if a.Type == nil {
				return &errUnsupported{typ: t.String()}
			}
			if err := renderValue(sb, a.Type, v.Index(i), true); err != nil {
				return err
			}
		}
	case reflect.Map:
		// A named tuple: the driver gives map[string]any, so the order comes
		// from the type.
		for i, a := range t.Args {
			if i > 0 {
				sb.WriteByte(',')
			}
			if a.Type == nil || a.Name == "" {
				return &errUnsupported{typ: t.String()}
			}
			if err := renderValue(sb, a.Type, v.MapIndex(reflect.ValueOf(a.Name)), true); err != nil {
				return err
			}
		}
	default:
		return typeMismatch(t, v)
	}
	return nil
}

// renderGoMap renders a `Map` value that the driver gave as a Go map, which
// happens when the map is nested in another type. A Go map has no order, so
// only a map with less than two entries has a deterministic text.
func renderGoMap(sb *strings.Builder, t *chType, v reflect.Value) error {
	kt, vt := t.typeArg(0), t.typeArg(1)
	if kt == nil || vt == nil || v.Kind() != reflect.Map {
		return typeMismatch(t, v)
	}
	if v.Len() > 1 {
		return &errUnsupported{typ: t.String() + " (nested, with more than one entry: the driver does not keep the order)"}
	}
	sb.WriteByte('{')
	it := v.MapRange()
	for it.Next() {
		if err := renderValue(sb, kt, it.Key(), true); err != nil {
			return err
		}
		sb.WriteByte(':')
		if err := renderValue(sb, vt, it.Value(), true); err != nil {
			return err
		}
	}
	sb.WriteByte('}')
	return nil
}

func renderPoint(sb *strings.Builder, p orb.Point) {
	sb.WriteByte('(')
	sb.WriteString(formatFloat(p[0], 64))
	sb.WriteByte(',')
	sb.WriteString(formatFloat(p[1], 64))
	sb.WriteByte(')')
}

// writeString writes s with the escapes of ClickHouse's
// writeAnyEscapedString (with the single quote as the quote character),
// quoted when nested.
func writeString(sb *strings.Builder, s string, nested bool) {
	if nested {
		sb.WriteByte('\'')
	}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\':
			sb.WriteString(`\\`)
		case '\'':
			sb.WriteString(`\'`)
		case '\b':
			sb.WriteString(`\b`)
		case '\f':
			sb.WriteString(`\f`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		case 0:
			sb.WriteString(`\0`)
		default:
			sb.WriteByte(c)
		}
	}
	if nested {
		sb.WriteByte('\'')
	}
}

// writeQuotedIf writes text that has no characters to escape (dates, UUIDs,
// addresses), quoted when nested.
func writeQuotedIf(sb *strings.Builder, s string, nested bool) {
	if nested {
		sb.WriteByte('\'')
		sb.WriteString(s)
		sb.WriteByte('\'')
		return
	}
	sb.WriteString(s)
}

// formatFloat writes f as ClickHouse does: the shortest text that reads back
// as the same value, in positional notation when the decimal exponent is in
// [-6, 21), else in scientific notation without a `+` sign or exponent padding
// (`1e21`, `1.5e-7`). This is the same layout as JavaScript's
// Number.prototype.toString.
func formatFloat(f float64, bitSize int) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case f == 0:
		if math.Signbit(f) {
			return "-0"
		}
		return "0"
	}
	// Shortest digits and exponent: d.ddddde±XX.
	s := strconv.FormatFloat(f, 'e', -1, bitSize)
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	mantissa, expText, _ := strings.Cut(s, "e")
	exp, _ := strconv.Atoi(expText)
	digits := strings.Replace(mantissa, ".", "", 1)
	// The value is 0.digits × 10^n.
	n := exp + 1
	k := len(digits)

	var out string
	switch {
	case k <= n && n <= 21:
		out = digits + strings.Repeat("0", n-k)
	case 0 < n && n <= 21:
		out = digits[:n] + "." + digits[n:]
	case -6 < n && n <= 0:
		out = "0." + strings.Repeat("0", -n) + digits
	default:
		e := n - 1
		out = digits[:1]
		if k > 1 {
			out += "." + digits[1:]
		}
		out += "e" + strconv.Itoa(e)
	}
	if neg {
		return "-" + out
	}
	return out
}

func formatDateTime64(t time.Time, precision int) string {
	s := t.Format("2006-01-02 15:04:05")
	if precision == 0 {
		return s
	}
	frac := fmt.Sprintf("%09d", t.Nanosecond())
	return s + "." + frac[:precision]
}

const maxTimeSeconds = 999*3600 + 59*60 + 59

// formatTime writes a `Time` / `Time64` value: [-]HH:MM:SS[.fff]. The hours
// can be more than 24.
func formatTime(d time.Duration, precision int) string {
	var sb strings.Builder
	if d < 0 {
		sb.WriteByte('-')
		d = -d
	}
	total := int64(d / time.Second)
	frac := int64(d % time.Second)
	// The server writes a value above 999:59:59 as 999:59:59, with a zero
	// fraction; the stored value is not changed.
	if total > maxTimeSeconds {
		total, frac = maxTimeSeconds, 0
	}
	fmt.Fprintf(&sb, "%02d:%02d:%02d", total/3600, total/60%60, total%60)
	if precision > 0 {
		sb.WriteString("." + fmt.Sprintf("%09d", frac)[:precision])
	}
	return sb.String()
}

// formatIPv6 follows ClickHouse's `formatIPv6`: the longest run of two or more
// zero words becomes `::`, and an IPv4-mapped (`::ffff:a.b.c.d`) or
// IPv4-compatible (`::a.b.c.d`) address ends in dotted form.
func formatIPv6(ip net.IP) string {
	var words [8]uint16
	for i := range words {
		words[i] = uint16(ip[2*i])<<8 | uint16(ip[2*i+1])
	}
	bestBase, bestLen := -1, 0
	for i := 0; i < 8; {
		if words[i] != 0 {
			i++
			continue
		}
		j := i
		for j < 8 && words[j] == 0 {
			j++
		}
		if j-i > bestLen {
			bestBase, bestLen = i, j-i
		}
		i = j
	}
	if bestLen < 2 {
		bestBase = -1
	}

	ipv4Compatible := bestBase == 0 && bestLen == 6
	ipv4Mapped := bestBase == 0 && bestLen == 5 && words[5] == 0xffff

	var sb strings.Builder
	for i := 0; i < 8; i++ {
		if bestBase != -1 && i >= bestBase && i < bestBase+bestLen {
			if i == bestBase {
				sb.WriteByte(':')
			}
			continue
		}
		if i != 0 {
			sb.WriteByte(':')
		}
		if i == 6 && (ipv4Compatible || ipv4Mapped) {
			sb.WriteString(net.IP(ip[12:16]).String())
			return sb.String()
		}
		sb.WriteString(strconv.FormatUint(uint64(words[i]), 16))
	}
	if bestBase != -1 && bestBase+bestLen == 8 {
		sb.WriteByte(':')
	}
	return sb.String()
}
