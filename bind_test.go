package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestFormatValueBoolAsText pins the #1891 fix at the formatter level: in the
// server-side query-parameter context (boolAsText=true) a bool — at any nesting
// depth — must render as `true`/`false`, while the legacy in-SQL bind context
// (boolAsText=false, used by ?/$1/@name substitution) must keep rendering it as
// `1`/`0`.
func TestFormatValueBoolAsText(t *testing.T) {
	tru, fls := true, false
	cases := []struct {
		name       string
		value      any
		queryParam string // boolAsText=true  (server-side {name:Type} parameter)
		bindSQL    string // boolAsText=false (legacy in-SQL substitution)
	}{
		{"scalar true", true, "true", "1"},
		{"scalar false", false, "false", "0"},
		{"Array(Bool)", []bool{true, false}, "[true, false]", "[1, 0]"},
		{"Array(Array(Bool))", [][]bool{{true}, {false}}, "[[true], [false]]", "[[1], [0]]"},
		// nullable bool in an array: non-null elements switch, nil stays NULL
		{"Array(Nullable(Bool))", []*bool{&tru, nil, &fls}, "[true, NULL, false]", "[1, NULL, 0]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := formatValue(time.UTC, Seconds, tc.value, true)
			require.NoError(t, err)
			assert.Equal(t, tc.queryParam, got, "server-side query-parameter formatting")

			got, err = formatValue(time.UTC, Seconds, tc.value, false)
			require.NoError(t, err)
			assert.Equal(t, tc.bindSQL, got, "legacy in-SQL bind formatting must be unchanged")
		})
	}
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
