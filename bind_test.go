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
	if assert.NoError(t, err) {
		assets := []struct {
			query    string
			params   []interface{}
			expected string
		}{
			{
				query:    "SELECT $1",
				params:   []interface{}{1},
				expected: "SELECT 1",
			},
			{
				query:    "SELECT $2 $1 $3",
				params:   []interface{}{1, 2, 3},
				expected: "SELECT 2 1 3",
			},
			{
				query:    "SELECT $2 $1 $3",
				params:   []interface{}{"a", "b", "c"},
				expected: "SELECT 'b' 'a' 'c'",
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
	if assert.NoError(t, err) {
		assets := []struct {
			query    string
			params   []interface{}
			expected string
		}{
			{
				query: "SELECT @col1",
				params: []interface{}{
					Named("col1", 1),
				},
				expected: "SELECT 1",
			},
			{
				query: "SELECT @col2 @col1 @col3",
				params: []interface{}{
					Named("col1", 1),
					Named("col2", 2),
					Named("col3", 3),
				},
				expected: "SELECT 2 1 3",
			},
			{
				query: "SELECT @col2 @col1 @col3",
				params: []interface{}{
					Named("col1", "a"),
					Named("col2", "b"),
					Named("col3", "c"),
				},
				expected: "SELECT 'b' 'a' 'c'",
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
			params   []interface{}
			expected string
		}{
			{
				query:    "SELECT ?",
				params:   []interface{}{1},
				expected: "SELECT 1",
			},
			{
				query:    "SELECT ? ? ?",
				params:   []interface{}{1, 2, 3},
				expected: "SELECT 1 2 3",
			},
			{
				query:    "SELECT ? ? ?",
				params:   []interface{}{"a", "b", "c"},
				expected: "SELECT 'a' 'b' 'c'",
			},
			{
				query:    "SELECT ? ? '\\?'",
				params:   []interface{}{"a", "b"},
				expected: "SELECT 'a' 'b' '?'",
			},
			{
				query:    "SELECT x where col = 'blah\\?' AND col2 = ?",
				params:   []interface{}{"a"},
				expected: "SELECT x where col = 'blah?' AND col2 = 'a'",
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
	`, 1, 2, "I'm a string param", nil)
	assert.Error(t, err)
}

func TestFormatTime(t *testing.T) {
	var (
		t1, _   = time.Parse("2006-01-02 15:04:05", "2022-01-12 15:00:00")
		tz, err = time.LoadLocation("Europe/London")
	)
	if assert.NoError(t, err) {
		val, _ := format(t1.Location(), t1)
		if assert.Equal(t, "toDateTime('2022-01-12 15:00:00')", val) {
			val, _ = format(tz, t1)
			assert.Equal(t, "toDateTime('2022-01-12 15:00:00', 'UTC')", val)
		}
	}
}

func TestStringBasedType(t *testing.T) {
	type (
		SupperString       string
		SupperSupperString string
	)
	val, _ := format(time.UTC, SupperString("a"))
	require.Equal(t, "'a'", val)
	val, _ = format(time.UTC, SupperSupperString("a"))
	require.Equal(t, "'a'", val)
	val, _ = format(time.UTC, []SupperSupperString{"a", "b", "c"})
	require.Equal(t, "'a', 'b', 'c'", val)
}

func TestFormatTuple(t *testing.T) {
	val, _ := format(time.UTC, []interface{}{"A", 1})
	assert.Equal(t, "('A', 1)", val)
	{
		tuples := [][]interface{}{
			[]interface{}{"A", 1},
			[]interface{}{"B", 2},
		}
		val, _ = format(time.UTC, tuples)
		assert.Equal(t, "('A', 1), ('B', 2)", val)
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
