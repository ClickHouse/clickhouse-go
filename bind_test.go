package clickhouse

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func TestBindNumeric(t *testing.T) {
	_, err := bind(time.Local, `
	SELECT * FROM t WHERE col = $1
		AND col2 = $2
		AND col3 = $1
		ANS col4 = $3
		AND null_coll = $4
	)
	`, 1, 2, "I'm a string param", nil)
	var nilPtr *bool = nil
	var nilPtrPtr **any = nil
	valuedPtr := &([]any{123}[0])
	nilValuePtr := &([]any{nil}[0])
	_, err = bind(time.Local, `
	SELECT * FROM t WHERE col = $5
		AND col2 = $2
		AND col3 = $1
		AND col4 = $3
		AND col5 = $4
	`, nilPtr, valuedPtr, nilPtrPtr, nilValuePtr, &nilValuePtr)
	assert.NoError(t, err)

	if assert.NoError(t, err) {
		assets := []struct {
			query    string
			params   []any
			expected string
		}{
			{
				query:    "SELECT $1",
				params:   []any{1},
				expected: "SELECT 1",
			},
			{
				query:    "SELECT $2 $1 $3",
				params:   []any{1, 2, 3},
				expected: "SELECT 2 1 3",
			},
			{
				query:    "SELECT $2 $1 $3",
				params:   []any{"a", "b", "c"},
				expected: "SELECT 'b' 'a' 'c'",
			},
			{
				query:    "SELECT $2 $1",
				params:   []any{true, false},
				expected: "SELECT 0 1",
			},
		}

		for _, asset := range assets {
			if actual, err := bind(time.Local, asset.query, asset.params...); assert.NoError(t, err) {
				assert.Equal(t, asset.expected, actual)
			}
		}
	}
}

func TestBindNamed(t *testing.T) {
	_, err := bind(time.Local, `
	SELECT * FROM t WHERE col = @col1
		AND col2 = @col2
		AND col3 = @col1
		ANS col4 = @col3
		AND col  @> 42
		AND null_coll = @col4
	)
	`,
		Named("col1", 1),
		Named("col2", 2),
		Named("col3", "I'm a string param"),
		Named("col4", nil),
	)
	var nilPtr *bool = nil
	var nilPtrPtr **any = nil
	valuedPtr := &([]any{123}[0])
	nilValuePtr := &([]any{nil}[0])
	_, err = bind(time.Local, `
	SELECT * FROM t WHERE col =  @col1
		AND col2 =  @col2
		AND col3 =  @col3
		AND col4 =  @col4
		AND col5 =  @col5
	`,
		Named("col1", nilPtr),
		Named("col2", nilPtrPtr),
		Named("col3", valuedPtr),
		Named("col4", nilValuePtr),
		Named("col5", &nilValuePtr))
	assert.NoError(t, err)

	if assert.NoError(t, err) {
		assets := []struct {
			query    string
			params   []any
			expected string
		}{
			{
				query: "SELECT @col1",
				params: []any{
					Named("col1", 1),
				},
				expected: "SELECT 1",
			},
			{
				query: "SELECT @col2 @col1 @col3",
				params: []any{
					Named("col1", 1),
					Named("col2", 2),
					Named("col3", 3),
				},
				expected: "SELECT 2 1 3",
			},
			{
				query: "SELECT @col2 @col1 @col3",
				params: []any{
					Named("col1", "a"),
					Named("col2", "b"),
					Named("col3", "c"),
				},
				expected: "SELECT 'b' 'a' 'c'",
			},
			{
				query: "SELECT @col2 @col1",
				params: []any{
					Named("col1", true),
					Named("col2", false),
				},
				expected: "SELECT 0 1",
			},
		}
		for _, asset := range assets {
			if actual, err := bind(time.Local, asset.query, asset.params...); assert.NoError(t, err) {
				assert.Equal(t, asset.expected, actual)
			}
		}
	}
}

func TestBindPositional(t *testing.T) {
	_, err := bind(time.Local, `
	SELECT * FROM t WHERE col = ?
		AND col2 = ?
		AND col3 = ?
		ANS col4 = ?
		AND null_coll = ?
	)
	`, 1, 2, 1, "I'm a string param", nil)
	if assert.NoError(t, err) {
		assets := []struct {
			query    string
			params   []any
			expected string
		}{
			{
				query:    "SELECT ?",
				params:   []any{1},
				expected: "SELECT 1",
			},
			{
				query:    "SELECT ? ? ?",
				params:   []any{1, 2, 3},
				expected: "SELECT 1 2 3",
			},
			{
				query:    "SELECT ? ? ?",
				params:   []any{"a", "b", "c"},
				expected: "SELECT 'a' 'b' 'c'",
			},
			{
				query:    "SELECT ? ? '\\?'",
				params:   []any{"a", "b"},
				expected: "SELECT 'a' 'b' '?'",
			},
			{
				query:    "SELECT x where col = 'blah\\?' AND col2 = ?",
				params:   []any{"a"},
				expected: "SELECT x where col = 'blah?' AND col2 = 'a'",
			},
			{
				query:    "SELECT `field\\?` WHERE id = ?",
				params:   []any{42},
				expected: "SELECT `field\\?` WHERE id = 42",
			},
			{
				query:    `SELECT "field\?" WHERE id = ?`,
				params:   []any{42},
				expected: `SELECT "field\?" WHERE id = 42`,
			},
			{
				query:    "SELECT * FROM (SELECT '1' AS `field?`) WHERE `field?` = ?",
				params:   []any{"1"},
				expected: "SELECT * FROM (SELECT '1' AS `field?`) WHERE `field?` = '1'",
			},
			{
				query:    "SELECT * FROM t WHERE name = 'foo?bar' AND id = ?",
				params:   []any{42},
				expected: "SELECT * FROM t WHERE name = 'foo?bar' AND id = 42",
			},
			{
				query:    `SELECT * FROM t WHERE "field?" = ?`,
				params:   []any{"value"},
				expected: `SELECT * FROM t WHERE "field?" = 'value'`,
			},
			{
				query:    "SELECT 'it''s ? ok' WHERE id = ?",
				params:   []any{42},
				expected: "SELECT 'it''s ? ok' WHERE id = 42",
			},
			{
				query:    "SELECT ? ?",
				params:   []any{true, false},
				expected: "SELECT 1 0",
			},
		}

		for _, asset := range assets {
			if actual, err := bind(time.Local, asset.query, asset.params...); assert.NoError(t, err) {
				assert.Equal(t, asset.expected, actual)
			}
		}
	}

	_, err = bind(time.Local, `
	SELECT * FROM t WHERE col = ?
		AND col2 = ?
		AND col3 = ?
		ANS col4 = ?
		AND null_coll = ?
	)
	`, 1, 2, "I'm a string param", nil, Named("namedArg", nil))
	assert.Error(t, err)

	var nilPtr *bool = nil
	var nilPtrPtr **any = nil
	valuedPtr := &([]any{123}[0])
	nilValuePtr := &([]any{nil}[0])

	_, err = bind(time.Local, `
	SELECT * FROM t WHERE col = ?
		AND col2 = ?
		AND col3 = ?
		AND col4 = ?
		AND col5 = ?
	`, nilPtr, valuedPtr, nilPtrPtr, nilValuePtr, &nilValuePtr)
	assert.NoError(t, err)
}

func TestBindNumericQuotedContexts(t *testing.T) {
	assets := []struct {
		query    string
		params   []any
		expected string
	}{
		{
			query:    "SELECT '$1', $1",
			params:   []any{42},
			expected: "SELECT '$1', 42",
		},
		{
			query:    "SELECT `$1`, $1",
			params:   []any{"value"},
			expected: "SELECT `$1`, 'value'",
		},
		{
			query:    `SELECT "$1", $1`,
			params:   []any{true},
			expected: `SELECT "$1", 1`,
		},
		{
			query:    "SELECT 'it''s $1 ok', $1",
			params:   []any{42},
			expected: "SELECT 'it''s $1 ok', 42",
		},
	}

	for _, asset := range assets {
		actual, err := bind(time.Local, asset.query, asset.params...)
		require.NoError(t, err)
		assert.Equal(t, asset.expected, actual)
	}
}

func TestBindMixedParamsFormatsQuotedContexts(t *testing.T) {
	assets := []struct {
		query    string
		params   []any
		expected string
	}{
		{
			query:    "SELECT '$1', ?",
			params:   []any{42},
			expected: "SELECT '$1', 42",
		},
		{
			query:    "SELECT '?', $1",
			params:   []any{42},
			expected: "SELECT '?', 42",
		},
		{
			query:    "SELECT `$1?`, ?",
			params:   []any{"value"},
			expected: "SELECT `$1?`, 'value'",
		},
		{
			query:    `SELECT "$1?", $1`,
			params:   []any{"value"},
			expected: `SELECT "$1?", 'value'`,
		},
	}

	for _, asset := range assets {
		actual, err := bind(time.Local, asset.query, asset.params...)
		require.NoError(t, err)
		assert.Equal(t, asset.expected, actual)
	}
}

func TestBindPositionalComments(t *testing.T) {
	assets := []struct {
		query    string
		params   []any
		expected string
	}{
		{
			query:    "SELECT ? FROM t -- comment ?\nWHERE x = ?",
			params:   []any{1, 2},
			expected: "SELECT 1 FROM t -- comment ?\nWHERE x = 2",
		},
		{
			query:    "SELECT ? # comment ?\nWHERE x = ?",
			params:   []any{1, 2},
			expected: "SELECT 1 # comment ?\nWHERE x = 2",
		},
		{
			query:    "SELECT ? /* ? ignored */, ?",
			params:   []any{1, 2},
			expected: "SELECT 1 /* ? ignored */, 2",
		},
		{
			// Block comments nest in ClickHouse: every ? until the outermost
			// "*/" is part of the comment.
			query:    "SELECT ? /* a /* ? */ ? */, ?",
			params:   []any{1, 2},
			expected: "SELECT 1 /* a /* ? */ ? */, 2",
		},
		{
			// A backslash inside a comment is left verbatim, not unescaped.
			query:    "SELECT ? -- \\?\n",
			params:   []any{1},
			expected: "SELECT 1 -- \\?\n",
		},
	}

	for _, asset := range assets {
		actual, err := bind(time.Local, asset.query, asset.params...)
		require.NoError(t, err)
		assert.Equal(t, asset.expected, actual)
	}
}

func TestBindPositionalEscapedQuestionInIdentifier(t *testing.T) {
	// "\?" must stay verbatim inside identifier quotes and comments; only inside
	// single-quoted string literals (and raw text) is the backslash dropped.
	assets := []struct {
		query    string
		params   []any
		expected string
	}{
		{
			query:    "SELECT `a\\?b`, ?",
			params:   []any{42},
			expected: "SELECT `a\\?b`, 42",
		},
		{
			query:    `SELECT "a\?b", ?`,
			params:   []any{42},
			expected: `SELECT "a\?b", 42`,
		},
		{
			query:    "SELECT 'a\\?b', ?",
			params:   []any{42},
			expected: "SELECT 'a?b', 42",
		},
	}

	for _, asset := range assets {
		actual, err := bind(time.Local, asset.query, asset.params...)
		require.NoError(t, err)
		assert.Equal(t, asset.expected, actual)
	}
}

func TestBindNumericComments(t *testing.T) {
	assets := []struct {
		query    string
		params   []any
		expected string
	}{
		{
			// $2 only appears in a comment, so a single arg is sufficient and
			// must not raise "have no arg for $2".
			query:    "SELECT $1 -- $2\n, $1",
			params:   []any{42},
			expected: "SELECT 42 -- $2\n, 42",
		},
		{
			query:    "SELECT $1 /* $2 */",
			params:   []any{42},
			expected: "SELECT 42 /* $2 */",
		},
	}

	for _, asset := range assets {
		actual, err := bind(time.Local, asset.query, asset.params...)
		require.NoError(t, err)
		assert.Equal(t, asset.expected, actual)
	}
}

func TestBindNamedQuotedContexts(t *testing.T) {
	assets := []struct {
		query    string
		params   []any
		expected string
	}{
		{
			query:    "SELECT `col@x`, @id",
			params:   []any{Named("id", 42)},
			expected: "SELECT `col@x`, 42",
		},
		{
			query:    "SELECT 'literal @name', @id",
			params:   []any{Named("id", 7)},
			expected: "SELECT 'literal @name', 7",
		},
		{
			query:    `SELECT "col@x", @id`,
			params:   []any{Named("id", 7)},
			expected: `SELECT "col@x", 7`,
		},
		{
			query:    "SELECT @id -- @ignored\n",
			params:   []any{Named("id", 7)},
			expected: "SELECT 7 -- @ignored\n",
		},
		{
			query:    "SELECT @id # @ignored\n",
			params:   []any{Named("id", 7)},
			expected: "SELECT 7 # @ignored\n",
		},
		{
			query:    "SELECT @id /* @ignored */",
			params:   []any{Named("id", 7)},
			expected: "SELECT 7 /* @ignored */",
		},
	}

	for _, asset := range assets {
		actual, err := bind(time.Local, asset.query, asset.params...)
		require.NoError(t, err)
		assert.Equal(t, asset.expected, actual)
	}
}

func TestBindNamedUnbound(t *testing.T) {
	// A genuinely missing named parameter (outside any quoted context) must
	// still error through the scanner-based binder.
	_, err := bind(time.Local, "SELECT @a, @b", Named("a", 1))
	require.Error(t, err)
}

func TestFormatTime(t *testing.T) {
	var (
		t1, _   = time.Parse("2006-01-02 15:04:05", "2022-01-12 15:00:00")
		tz, err = time.LoadLocation("Europe/London")
	)
	if assert.NoError(t, err) {
		val, _ := format(t1.Location(), Seconds, t1)
		if assert.Equal(t, "toDateTime('2022-01-12 15:00:00')", val) {
			val, _ = format(tz, Seconds, t1)
			assert.Equal(t, "toDateTime('2022-01-12 15:00:00', 'UTC')", val)
		}

		// test with pointer to time.Time
		val, _ = format(t1.Location(), Seconds, &t1)
		assert.Equal(t, "toDateTime('2022-01-12 15:00:00')", val)
	}

	// test with nil pointer to time.Time
	val, _ := format(time.UTC, Seconds, (*time.Time)(nil))
	assert.Equal(t, "NULL", val)
}

// TestFormatFloat is a regression test for issue #1862: a bound float64/float32
// used to fall through to fmt.Sprint, so 1.0 rendered as the bare literal "1"
// (which ClickHouse narrows to an integer type) and non-finite values rendered
// as "NaN"/"+Inf" which ClickHouse cannot parse.
func TestFormatFloat(t *testing.T) {
	cases := []struct {
		param    any
		expected string
	}{
		{float64(1.0), "cast(1, 'Float64')"},
		{float64(1.5), "cast(1.5, 'Float64')"},
		{float64(-2), "cast(-2, 'Float64')"},
		{float32(1.0), "cast(1, 'Float32')"},
		{float32(2.5), "cast(2.5, 'Float32')"},
		{math.Inf(1), "cast('inf', 'Float64')"},
		{math.Inf(-1), "cast('-inf', 'Float64')"},
		{math.NaN(), "cast('nan', 'Float64')"},
	}
	for _, c := range cases {
		val, err := format(time.UTC, Seconds, c.param)
		require.NoError(t, err)
		assert.Equal(t, c.expected, val)
	}
}

func TestFormatScaledTime(t *testing.T) {
	var (
		t1, _   = time.Parse("2006-01-02 15:04:05.000000000", "2022-01-12 15:00:00.123456789")
		tz, err = time.LoadLocation("Europe/London")
	)
	require.NoError(t, err)
	// seconds
	val, _ := format(t1.Location(), Seconds, t1)
	require.Equal(t, "toDateTime('2022-01-12 15:00:00')", val)
	val, _ = format(t1.Location(), Seconds, t1.In(time.Now().Location()))
	require.Equal(t, "toDateTime('1641999600')", val)
	val, _ = format(t1.Location(), Seconds, time.Unix(0, 0))
	require.Equal(t, "toDateTime(0)", val)
	val, _ = format(tz, Seconds, t1)
	require.Equal(t, "toDateTime('2022-01-12 15:00:00', 'UTC')", val)
	// milliseconds
	val, _ = format(t1.Location(), MilliSeconds, t1)
	require.Equal(t, "toDateTime64('2022-01-12 15:00:00.123', 3)", val)
	val, _ = format(t1.Location(), MilliSeconds, t1.In(time.Now().Location()))
	require.Equal(t, "toDateTime64('1641999600123', 3)", val)
	val, _ = format(t1.Location(), MilliSeconds, time.Unix(0, 0))
	require.Equal(t, "toDateTime(0)", val)
	val, _ = format(tz, MilliSeconds, t1)
	require.Equal(t, "toDateTime64('2022-01-12 15:00:00.123', 3, 'UTC')", val)
	// microseconds
	val, _ = format(t1.Location(), MicroSeconds, t1)
	require.Equal(t, "toDateTime64('2022-01-12 15:00:00.123456', 6)", val)
	val, _ = format(t1.Location(), MicroSeconds, t1.In(time.Now().Location()))
	require.Equal(t, "toDateTime64('1641999600123456', 6)", val)
	val, _ = format(t1.Location(), MicroSeconds, time.Unix(0, 0))
	require.Equal(t, "toDateTime(0)", val)
	val, _ = format(tz, MicroSeconds, t1)
	require.Equal(t, "toDateTime64('2022-01-12 15:00:00.123456', 6, 'UTC')", val)
	// nanoseconds
	val, _ = format(t1.Location(), NanoSeconds, t1)
	require.Equal(t, "toDateTime64('2022-01-12 15:00:00.123456789', 9)", val)
	val, _ = format(t1.Location(), NanoSeconds, t1.In(time.Now().Location()))
	require.Equal(t, "toDateTime64('1641999600123456789', 9)", val)
	val, _ = format(t1.Location(), NanoSeconds, time.Unix(0, 0))
	require.Equal(t, "toDateTime(0)", val)
	val, _ = format(tz, NanoSeconds, t1)
	require.Equal(t, "toDateTime64('2022-01-12 15:00:00.123456789', 9, 'UTC')", val)
}

func TestStringBasedType(t *testing.T) {
	type (
		SupperString       string
		SupperSupperString string
	)
	val, _ := format(time.UTC, Seconds, SupperString("a"))
	require.Equal(t, "'a'", val)
	val, _ = format(time.UTC, Seconds, SupperSupperString("a"))
	require.Equal(t, "'a'", val)
	val, _ = format(time.UTC, Seconds, []SupperSupperString{"a", "b", "c"})
	require.Equal(t, "['a', 'b', 'c']", val)
}

func TestFormatGroup(t *testing.T) {
	groupSet := GroupSet{Value: []any{"A", 1}}
	val, _ := format(time.UTC, Seconds, groupSet)
	assert.Equal(t, "('A', 1)", val)
	{
		tuples := []GroupSet{
			{Value: []any{"A", 1}},
			{Value: []any{"B", 2}},
		}
		val, _ = format(time.UTC, Seconds, tuples)
		assert.Equal(t, "('A', 1), ('B', 2)", val)
	}
}

func TestFormatArray(t *testing.T) {
	arraySet := ArraySet{"A", 1}
	val, _ := format(time.UTC, Seconds, arraySet)
	assert.Equal(t, "['A', 1]", val)
}

func TestFormatMap(t *testing.T) {
	val, _ := format(time.UTC, Seconds, map[string]uint8{"a": 1})
	assert.Equal(t, "map('a', 1)", val)
}

func TestFormatMapEscapesStringKeys(t *testing.T) {
	val, err := format(time.UTC, Seconds, map[string]uint8{`a'b\c`: 1})
	require.NoError(t, err)
	assert.Equal(t, `map('a\'b\\c', 1)`, val)
}

func TestTimezoneSQLEscaping(t *testing.T) {
	t.Run("prevent SQL injection via timezone name", func(t *testing.T) {
		maliciousLoc := time.FixedZone("UTC') UNION ALL SELECT 1,2,3 --", 0)
		time.LoadLocation(maliciousLoc.String())
		maliciousTime := time.Now().In(maliciousLoc)

		val, err := format(time.UTC, Seconds, maliciousTime)
		require.Error(t, err)
		assert.Equal(t, "", val)
		assert.ErrorIs(t, err, ErrInvalidTimezone)
	})

	t.Run("prevent SQL injection via timezone name with milliseconds", func(t *testing.T) {
		maliciousLoc := time.FixedZone("America/New_York'); DROP TABLE users; --", 0)
		maliciousTime := time.Now().In(maliciousLoc)

		val, err := format(time.UTC, MilliSeconds, maliciousTime)
		require.Error(t, err)
		assert.Equal(t, "", val)
		assert.ErrorIs(t, err, ErrInvalidTimezone)
	})

	t.Run("prevent SQL injection via timezone with backslashes", func(t *testing.T) {
		maliciousLoc := time.FixedZone(`UTC\' OR 1=1 --`, 0)
		maliciousTime := time.Now().In(maliciousLoc)

		val, err := format(time.UTC, Seconds, maliciousTime)
		// require.NoError(t, err)
		require.Error(t, err)
		assert.Equal(t, "", val)
		assert.ErrorIs(t, err, ErrInvalidTimezone)
	})

	t.Run("normal timezone names remain unaffected", func(t *testing.T) {
		// Test that normal, safe timezone names still work correctly
		normalLoc := time.FixedZone("America/New_York", -5*3600)
		normalTime := time.Now().In(normalLoc)

		val, err := format(time.UTC, Seconds, normalTime)
		require.NoError(t, err)

		// Should contain the timezone name without any escaping
		assert.Contains(t, val, "'America/New_York'")
		assert.NotContains(t, val, `\'`, "Normal timezone names should not have escaped quotes")
		assert.Contains(t, val, "toDateTime(")
	})

	// Query-parameter formatting writes times as epoch digits and never
	// includes the timezone name, so a malicious name has nothing to leak
	// into. These lock that in for both the nested and top-level paths.
	t.Run("timezone name never reaches query-parameter text", func(t *testing.T) {
		maliciousLoc := time.FixedZone("UTC') UNION ALL SELECT 1,2,3 --", 0)
		maliciousTime := time.Date(2020, 1, 2, 3, 4, 5, 0, maliciousLoc)

		// Nested inside a composite value.
		val, err := formatValue(time.UTC, Seconds, []time.Time{maliciousTime}, formatParamText)
		require.NoError(t, err)
		assert.Equal(t, "['1577934245']", val)

		// Top-level Named value.
		opts := &QueryOptions{}
		_, err = bindQueryOrAppendParameters(true, opts, "SELECT {d:DateTime}", time.UTC,
			driver.NamedValue{Name: "d", Value: maliciousTime})
		require.NoError(t, err)
		assert.Equal(t, "1577934245", opts.parameters["d"])
	})

	t.Run("malicious string stays escaped in query-parameter text", func(t *testing.T) {
		payload := `'} UNION ALL SELECT 1 --`
		val, err := formatValue(time.UTC, Seconds, map[string]string{"k": payload}, formatParamText)
		require.NoError(t, err)
		assert.Equal(t, `{'k':'\'} UNION ALL SELECT 1 --'}`, val)
	})
}

// a simple (non thread safe) ordered map, implementing the column.OrderedMap interface
type OrderedMap struct {
	keys   []any
	values map[any]any
}

func NewOrderedMap() *OrderedMap {
	om := OrderedMap{}
	om.keys = []any{}
	om.values = map[any]any{}
	return &om
}

func (om *OrderedMap) Get(key any) (any, bool) {
	if value, present := om.values[key]; present {
		return value, present
	}
	return nil, false
}

func (om *OrderedMap) Put(key any, value any) {
	if _, present := om.values[key]; present {
		om.values[key] = value
		return
	}
	om.keys = append(om.keys, key)
	om.values[key] = value
}

func (om *OrderedMap) Keys() <-chan any {
	ch := make(chan any)
	go func() {
		defer close(ch)
		for _, key := range om.keys {
			ch <- key
		}
	}()
	return ch
}

func TestFormatMapOrdered(t *testing.T) {
	om := NewOrderedMap()
	om.Put("b", 2)
	om.Put("a", 1)

	val, _ := format(time.UTC, Seconds, om)
	assert.Equal(t, "map('b', 2, 'a', 1)", val)
}

// TestFormatValueModes covers the fixes for #1891 and #1898. The server
// parses {name:Type} query parameters as text, not SQL: bools must be
// true/false instead of 1/0, maps {'k':v} instead of map('k', v), floats
// plain numbers instead of cast(...), times quoted epoch strings instead of
// toDateTime(...). Client-side binding (the ?/$1/@name placeholders) must
// keep the SQL forms. Both hold at any nesting depth.
func TestFormatValueModes(t *testing.T) {
	tru, fls := true, false
	ts := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		name       string
		value      any
		queryParam string // formatParamText (server-side {name:Type} parameter)
		bindSQL    string // formatSQL (client-side bind substitution)
	}{
		{"scalar true", true, "true", "1"},
		{"scalar false", false, "false", "0"},
		{"Array(Bool)", []bool{true, false}, "[true, false]", "[1, 0]"},
		{"Array(Array(Bool))", [][]bool{{true}, {false}}, "[[true], [false]]", "[[1], [0]]"},
		// nullable bools: real values change format, nil stays NULL
		{"Array(Nullable(Bool))", []*bool{&tru, nil, &fls}, "[true, NULL, false]", "[1, NULL, 0]"},
		// maps: text format vs map() SQL function (#1898)
		{"Map(String, Bool)", map[string]bool{"a": true}, "{'a':true}", "map('a', 1)"},
		{"Map(String, String)", map[string]string{"a": "b"}, "{'a':'b'}", "map('a', 'b')"},
		{"empty map", map[string]string{}, "{}", "map()"},
		{"Map string key escaping", map[string]uint8{`a'b\c`: 1}, `{'a\'b\\c':1}`, `map('a\'b\\c', 1)`},
		{"Map(String, Map(String, Bool))",
			map[string]map[string]bool{"a": {"x": true}},
			"{'a':{'x':true}}", "map('a', map('x', 1))"},
		{"Array(Map(String, Bool))",
			[]map[string]bool{{"a": true}, {"b": false}},
			"[{'a':true}, {'b':false}]", "[map('a', 1), map('b', 0)]"},
		{"Map(String, Array(Bool))",
			map[string][]bool{"a": {true, false}},
			"{'a':[true, false]}", "map('a', [1, 0])"},
		{"Map(Bool, String) key formatting", map[bool]string{true: "x"}, "{true:'x'}", "map(1, 'x')"},
		// floats: plain text vs cast() SQL function
		{"Float64", 1.5, "1.5", "cast(1.5, 'Float64')"},
		{"Float32", float32(1.5), "1.5", "cast(1.5, 'Float32')"},
		{"Float64 NaN", math.NaN(), "nan", "cast('nan', 'Float64')"},
		{"Float64 +Inf", math.Inf(1), "inf", "cast('inf', 'Float64')"},
		{"Float64 -Inf", math.Inf(-1), "-inf", "cast('-inf', 'Float64')"},
		{"Map(String, Float64)", map[string]float64{"a": 1.5}, "{'a':1.5}", "map('a', cast(1.5, 'Float64'))"},
		// nested times: quoted epoch (zone-free) vs toDateTime() SQL function
		{"Array(DateTime)", []time.Time{ts},
			"['1577934245']", "[toDateTime('2020-01-02 03:04:05')]"},
		{"Map(String, DateTime)", map[string]time.Time{"a": ts},
			"{'a':'1577934245'}", "map('a', toDateTime('2020-01-02 03:04:05'))"},
		// a sub-second time keeps its fraction in param mode (for DateTime64)
		{"Map(String, DateTime64)", map[string]time.Time{"a": ts.Add(123 * time.Millisecond)},
			"{'a':'1577934245.123'}", "map('a', toDateTime('2020-01-02 03:04:05'))"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := formatValue(time.UTC, Seconds, tc.value, formatParamText)
			require.NoError(t, err)
			assert.Equal(t, tc.queryParam, got, "server-side query-parameter formatting")

			got, err = formatValue(time.UTC, Seconds, tc.value, formatSQL)
			require.NoError(t, err)
			assert.Equal(t, tc.bindSQL, got, "client-side bind formatting must be unchanged")
		})
	}
}

// TestFormatTimeParam checks how a time.Time is rendered as a query
// parameter: epoch seconds, so the instant survives no matter which timezone
// the value carries or the parameter declares. Sub-second values keep their
// fraction trimmed to milli/micro/nanoseconds so a DateTime64 parameter
// doesn't lose precision.
func TestFormatTimeParam(t *testing.T) {
	base := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC) // epoch 1577934245
	tokyo := time.FixedZone("Asia/Tokyo", 9*3600)
	cases := []struct {
		name string
		in   time.Time
		want string
	}{
		{"whole seconds", base, "1577934245"},
		{"milliseconds", base.Add(123 * time.Millisecond), "1577934245.123"},
		{"microseconds", base.Add(123456 * time.Microsecond), "1577934245.123456"},
		{"nanoseconds", base.Add(123456789 * time.Nanosecond), "1577934245.123456789"},
		// the same instant expressed in another zone renders identically
		{"non-UTC zone, same instant", base.In(tokyo), "1577934245"},
		// pre-1970 instants: the sign must cover the fraction too
		// (-86400.5 seconds, not -86401 + 0.5)
		{"pre-1970 sub-second", time.Date(1969, 12, 30, 23, 59, 59, 500000000, time.UTC), "-86400.500"},
		{"pre-1970 whole second", time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), "-86400"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatTimeParam(tc.in))
		})
	}
}

// TestFormatTimeWithScale checks how a DateNamed value is rendered as a
// query parameter: epoch seconds (instant-preserving, like formatTimeParam)
// with exactly the fractional digits the explicit scale selects, truncating
// anything finer.
func TestFormatTimeWithScale(t *testing.T) {
	base := time.Date(2020, 1, 2, 3, 4, 5, 123456789, time.UTC) // epoch 1577934245.123456789
	tokyo := time.FixedZone("Asia/Tokyo", 9*3600)
	cases := []struct {
		name  string
		in    time.Time
		scale TimeUnit
		want  string
	}{
		{"Seconds truncates fraction", base, Seconds, "1577934245"},
		{"MilliSeconds", base, MilliSeconds, "1577934245.123"},
		{"MicroSeconds", base, MicroSeconds, "1577934245.123456"},
		{"NanoSeconds", base, NanoSeconds, "1577934245.123456789"},
		// fixed width even when the value is coarser than the scale
		{"whole second at MilliSeconds", base.Truncate(time.Second), MilliSeconds, "1577934245.000"},
		// the same instant expressed in another zone renders identically
		{"non-UTC zone, same instant", base.In(tokyo), MilliSeconds, "1577934245.123"},
		// pre-1970: the sign must cover the fraction too
		{"pre-1970 sub-second", time.Date(1969, 12, 30, 23, 59, 59, 500000000, time.UTC), MilliSeconds, "-86400.500"},
		{"pre-1970 whole second at MilliSeconds", time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), MilliSeconds, "-86400.000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatTimeWithScale(tc.in, tc.scale))
		})
	}
}

// TestNilQueryParameter checks that a nil sent as a query parameter becomes
// `\N` — the whole-text NULL marker. The `NULL` keyword only works nested
// inside composites; at the top level the server would read it as the string
// "NULL" or fail to parse it.
func TestNilQueryParameter(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  string
	}{
		{"untyped nil", nil, `\N`},
		{"nil *string", (*string)(nil), `\N`},
		{"nil *time.Time", (*time.Time)(nil), `\N`},
		{"nil *int", (*int)(nil), `\N`},
		// nils nested inside a composite keep the NULL keyword
		{"nil inside array", []*string{nil}, "[NULL]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := &QueryOptions{}
			_, err := bindQueryOrAppendParameters(true, opts, "SELECT {p:Nullable(String)}", time.UTC,
				driver.NamedValue{Name: "p", Value: tc.value})
			require.NoError(t, err)
			assert.Equal(t, tc.want, opts.parameters["p"])
		})
	}
}

// TestFormatValueModesOrderedMap checks that ordered maps switch syntax
// between the two modes just like plain Go maps do.
func TestFormatValueModesOrderedMap(t *testing.T) {
	om := NewOrderedMap()
	om.Put("b", true)
	om.Put("a", false)

	got, err := formatValue(time.UTC, Seconds, om, formatParamText)
	require.NoError(t, err)
	assert.Equal(t, "{'b':true,'a':false}", got)

	got, err = formatValue(time.UTC, Seconds, om, formatSQL)
	require.NoError(t, err)
	assert.Equal(t, "map('b', 1, 'a', 0)", got)
}

func TestBindNamedWithTernaryOperator(t *testing.T) {
	sqls := []string{
		`SELECT if(@arg1,@arg2,@arg3)`, // correct
		`SELECT @arg1?@arg2:@arg3`,     // failed here
	}
	for _, sql := range sqls {
		_, err := bind(time.Local, sql,
			Named("arg1", 0),
			Named("arg2", 1),
			Named("arg3", 2))
		assert.NoError(t, err)
	}
}

func BenchmarkBindNumeric(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := bind(time.Local, `
		SELECT * FROM t WHERE col = $1
			AND col2 = $2
			AND col3 = $1
			ANS col4 = $3
			AND null_coll = $4
		)
		`, 1, 2, "I'm a string param", nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBindPositional(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := bind(time.Local, `
		SELECT * FROM t WHERE col = ?
			AND col2 = ?
			AND col3 = ?
			ANS col4 = ?
			AND null_coll = ?
		)
		`, 1, 2, 1, "I'm a string param", nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBindNamed(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := bind(time.Local, `
		SELECT * FROM t WHERE col = @col1
			AND col2 = @col2
			AND col3 = @col1
			ANS col4 = @col3
			AND null_coll = @col4
		)
		`,
			Named("col1", 1),
			Named("col2", 2),
			Named("col3", "I'm a string param"),
			Named("col4", nil),
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}
