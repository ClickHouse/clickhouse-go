package orderedmap

import (
	"cmp"
	"github.com/stretchr/testify/assert"
	"slices"
	"testing"
)

func TestMap(t *testing.T) {
	m := map[int]int{1: 2, 3: 4, 5: 6, 7: 8, 9: 10, 11: 12, 13: 14, 15: 16, 17: 18, 19: 20}
	var keys []int
	var values []int
	var entries []entry[int, int]
	for k, v := range m {
		entries = append(entries, entry[int, int]{k, v})
	}
	slices.SortFunc(entries, func(a, b entry[int, int]) int { return cmp.Compare(a.key, b.key) })
	for _, e := range entries {
		keys = append(keys, e.key)
		values = append(values, e.value)
	}

	// Simple FromMap
	om1 := FromMap(m)

	// Collect go1.23+ iter.Seq2 iterator
	om2 := Collect(iterMap(m))

	// Manual fill (e.g. when the map is being read back from ClickHouse)
	om3 := new(Map[int, int])
	for _, e := range entries {
		om3.Put(e.key, e.value)
	}

	// Custom sort func
	omR := FromMapFunc(m, func(a, b int) int { return -cmp.Compare(a, b) })

	testMap := func(om *Map[int, int]) {
		assert.Equal(t, m, om.ToMap())
		assert.Equal(t, m, collectMap(om.All))
		assert.Equal(t, keys, collect(om.Keys))
		assert.Equal(t, values, collect(om.Values))
		iter, i := om.Iterator(), 0
		for iter.Next() {
			assert.Equal(t, keys[i], iter.Key())
			assert.Equal(t, values[i], iter.Value())
			i++
		}
	}

	testMap(om1)
	testMap(om2)
	testMap(om3)

	assert.Equal(t, m, omR.ToMap())
	keysR := slices.Clone(keys)
	slices.Reverse(keysR)
	assert.Equal(t, keysR, collect(omR.Keys))
}

// go1.23+ helper reimplementations
func iterMap[K comparable, V any, M ~map[K]V](m M) func(yield func(K, V) bool) {
	return func(yield func(K, V) bool) {
		for k, v := range m {
			if !yield(k, v) {
				break
			}
		}
	}
}

func collect[V any](seq func(yield func(V) bool)) (s []V) {
	seq(func(v V) bool {
		s = append(s, v)
		return true
	})
	return
}

func collectMap[K comparable, V any](seq func(yield func(K, V) bool)) (m map[K]V) {
	m = make(map[K]V)
	seq(func(k K, v V) bool {
		m[k] = v
		return true
	})
	return
}
