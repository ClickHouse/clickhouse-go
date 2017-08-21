package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NumInput(t *testing.T) {
	for query, num := range map[string]int{
		"SELECT * FROM example WHERE os_id = 42":                                                  0,
		"SELECT * FROM example WHERE email = 'name@mail'":                                         0,
		"SELECT * FROM example WHERE os_id = @os_id AND browser_id = @os_id":                      1,
		"SELECT * FROM example WHERE os_id = @os_id AND browser_id = @os_id2":                     2,
		"SELECT * FROM example WHERE os_id in (@os_id,@browser_id) browser_id = @browser_id":      2,
		"SELECT * FROM example WHERE os_id IN (@os_id, @browser_id) AND browser_id = @browser_id": 2,
		"SELECT * FROM example WHERE os_id = ? AND browser_id = ?":                                2,
		"SELECT * FROM example WHERE os_id in (?,?) browser_id = ?":                               3,
		"SELECT * FROM example WHERE os_id IN (?, ?) AND browser_id = ?":                          3,
		"SELECT a ? '+' : '-'":                                                                    0,
		"SELECT a ? '+' : '-' FROM example WHERE a = ? AND b IN(?)":                               2,
		`SELECT
			a ? '+' : '-'
		FROM example WHERE a = 42 and b in(
			?,
			?,
			?
		)
		`: 3,
	} {
		assert.Equal(t, num, numInput(query), query)
	}
}

func Benchmark_NumInput(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		numInput("SELECT * FROM example WHERE os_id in (@os_id,@browser_id) browser_id = @browser_id")
	}
}
