// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
