
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
