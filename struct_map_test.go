package clickhouse

import (
	"reflect"
	"strings"
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

func TestStructIdxTagOptions(t *testing.T) {
	type Example struct {
		Col1 string `ch:"col_1,option"`
	}

	index := structIdx(reflect.TypeOf(Example{}))
	assert.Equal(t, map[string][]int{
		"col_1": {0},
	}, index)
}

func TestStructColumns(t *testing.T) {
	type Embed2 struct {
		Col6 uint8
	}
	type Embed struct {
		Col4       string `ch:"named"`
		ColIgnored string `ch:"-"`
		Embed2
	}
	type Example struct {
		Col1        string
		Col2        time.Time `ch:"col_2"`
		ColWithOpts string    `ch:"col_3,json"`
		ignored     string
		Embed
		*Embed2
	}

	columns, err := StructColumns(Example{})
	assert.NoError(t, err)
	assert.Equal(t, []string{"Col1", "col_2", "col_3", "named", "Col6"}, columns)

	columns, err = StructColumns(&Example{})
	assert.NoError(t, err)
	assert.Equal(t, []string{"Col1", "col_2", "col_3", "named", "Col6"}, columns)
}

func TestStructColumnsReturnsCacheCopy(t *testing.T) {
	type Example struct {
		Col1 string
	}

	columns, err := StructColumns(Example{})
	assert.NoError(t, err)
	columns[0] = "mutated"

	columns, err = StructColumns(Example{})
	assert.NoError(t, err)
	assert.Equal(t, []string{"Col1"}, columns)
}

func TestStructColumnsErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    any
	}{
		{name: "nil", v: nil},
		{name: "string", v: "not a struct"},
		{name: "slice", v: []string{"not a struct"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			columns, err := StructColumns(tc.v)
			assert.Nil(t, columns)
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), "StructColumns"))
		})
	}
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
