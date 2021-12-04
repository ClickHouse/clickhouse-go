package clickhouse

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_NumInput(t *testing.T) {
	for query, num := range map[string]int{
		"SELECT * FROM example WHERE os_id = 42":                                                  0,
		"SELECT * FROM example WHERE email = 'name@mail'":                                         0,
		"SELECT * FROM example WHERE email = 'na`me@mail'":                                        0,
		"SELECT * FROM example WHERE email = 'na`m`e@mail'":                                       0,
		"SELECT * FROM example WHERE email = 'na`m`e@m`ail'":                                      0,
		"SELECT * FROM example WHERE os_id = @os_id AND browser_id = @os_id":                      1,
		"SELECT * FROM example WHERE os_id = @os_id AND browser_id = @os_id2":                     2,
		"SELECT * FROM example WHERE os_id in (@os_id,@browser_id) browser_id = @browser_id":      2,
		"SELECT * FROM example WHERE os_id IN (@os_id, @browser_id) AND browser_id = @browser_id": 2,
		"SELECT * FROM example WHERE os_id = ? AND browser_id = ?":                                2,
		"SELECT * FROM example WHERE os_id in (?,?) browser_id = ?":                               3,
		"SELECT * FROM example WHERE os_id IN (?, ?) AND browser_id = ?":                          3,
		"SELECT a ? '+' : '-'": 0,
		"SELECT a ? '+' : '-' FROM example WHERE a = ? AND b IN(?)": 2,
		`SELECT
			a ? '+' : '-'
		FROM example WHERE a = 42 and b in(
			?,
			?,
			?
		)
		`: 3,
		"SELECT * from EXAMPLE LIMIT ?":                                       1,
		"SELECT * from EXAMPLE LIMIT ?, ?":                                    2,
		"SELECT * from EXAMPLE LIMIT ? OFFSET ?":                              2,
		"SELECT * from EXAMPLE WHERE os_id like ?":                            1,
		"SELECT * FROM example WHERE a BETWEEN ? AND ?":                       2,
		"SELECT * FROM example WHERE a BETWEEN ? AND ? AND b = ?":             3,
		"SELECT * FROM example WHERE a = ? AND b BETWEEN ? AND ?":             3,
		"SELECT * FROM example WHERE a BETWEEN ? AND ? AND b BETWEEN ? AND ?": 4,
		"SELECT replace(a, '\\'', '\"') FROM example WHERE b = ?":             1,
		"SELECT * FROM example WHERE counter % ? = 0":                         1,
		"SELECT * FROM example WHERE modulo(counter, ?) = 0":                  1,
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

func Test_Quote(t *testing.T) {
	si := data.ServerInfo{Timezone: time.UTC}
	t1 := time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)
	tz, _ := time.LoadLocation("America/New_York")          //-5
	t2 := time.Date(2006, time.January, 2, 19, 0, 0, 0, tz) //UTC 2006 01-03 00:00:00
	for expected, value := range map[string]interface{}{
		"'a'":                    "a",
		"1":                      1,
		"'a', 'b', 'c'":          []string{"a", "b", "c"},
		"1, 2, 3, 4, 5":          []int{1, 2, 3, 4, 5},
		"toDateTime(1136214245)": t1,
		fmt.Sprintf("toDate(%d)", t2.Unix()/24/3600): t2,
	} {
		assert.Equal(t, expected, quote(value, si))
	}
}
