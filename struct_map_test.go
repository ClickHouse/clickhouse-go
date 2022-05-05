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
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStructIdx(t *testing.T) {
	type Embed2 struct {
		Col6 uint8
	}
	type Embed struct {
		Col4 string `ch:"named"`
		Embed2
	}
	type Example struct {
		Col1   string
		Col2   time.Time
		ColPtr *string
		Embed
		*Embed2
	}
	index := structIdx(reflect.TypeOf(Example{
		Col1: "X",
	}))
	assert.Equal(t, map[string][]int{
		"Col1":   {0},
		"Col2":   {1},
		"ColPtr": {2},
		"named":  {3, 0},
		"Col6":   {3, 1, 0},
	}, index)
}

func TestMapper(t *testing.T) {
	type Embed2 struct {
		Col6 uint8
	}
	type Embed struct {
		Col4 string `ch:"named"`
		Embed2
	}
	type Example struct {
		Col1   string
		Col2   time.Time
		ColPtr *string
		Embed
		*Embed2
	}
	mapper := structMap{}
	values, err := mapper.Map("", []string{"Col1", "named"}, &Example{
		Col1: "X",
		Embed: Embed{
			Col4: "Named value",
		},
	}, false)

	t.Log(values, err)
}

func BenchmarkStructMap(b *testing.B) {
	type Embed2 struct {
		Col6 uint8
	}
	type Embed struct {
		Col4 string `ch:"named"`
		Embed2
	}
	type Example struct {
		Col1   string
		Col2   time.Time
		ColPtr *string
		Embed
		*Embed2
	}
	var (
		mapper = structMap{}
		data   = &Example{
			Col1: "X",
			Embed: Embed{
				Col4: "Named value",
			},
		}
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := mapper.Map("", []string{"Col1", "named"}, data, false); err != nil {
			b.Fatal(err)
		}
	}
}
