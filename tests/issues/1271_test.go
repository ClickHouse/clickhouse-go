package issues

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

// test for https://github.com/ClickHouse/clickhouse-go/issues/1271
func Test1271(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	defer func() {
		conn.Exec(context.Background(), "DROP TABLE flush_with_close_query_example")
		conn.Close()
	}()
	conn.Exec(context.Background(), "DROP TABLE IF EXISTS flush_with_close_query_example")
	ctx := context.Background()
	err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS flush_with_close_query_example (
			  Col1 UInt64
			, Col2 String
			, Col3 FixedString(3)
			, Col4 UUID
			, Col5 Map(String, UInt64)
			, Col6 Array(String)
			, Col7 Tuple(String, UInt64, Array(Map(String, UInt64)))
			, Col8 DateTime
		) Engine = MergeTree() ORDER BY tuple()
	`)
	require.NoError(t, err)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO flush_with_close_query_example", driver.WithCloseOnFlush())
	require.NoError(t, err)
	// 1 million rows should only take < 1s on most desktops
	for i := 0; i < 100_000; i++ {
		require.NoError(t, batch.Append(
			uint64(i),
			RandAsciiString(5),
			RandAsciiString(3),
			uuid.New(),
			map[string]uint64{"key": uint64(i)}, // Map(String, UInt64)
			[]string{RandAsciiString(1), RandAsciiString(1), RandAsciiString(1), RandAsciiString(1), RandAsciiString(1), RandAsciiString(1)}, // Array(String)
			[]any{ // Tuple(String, UInt64, Array(Map(String, UInt64)))
				RandAsciiString(10), uint64(i), []map[string]uint64{
					{"key": uint64(i)},
					{"key": uint64(i + 1)},
					{"key": uint64(i) + 2},
				},
			},
			time.Now().Add(time.Duration(i)*time.Second),
		))
		if i > 0 && i%10000 == 0 {
			require.NoError(t, batch.Flush())
			PrintMemUsage()
		}
	}
	require.NoError(t, batch.Flush())
	// confirm we have the right count
	var col1 uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM flush_with_close_query_example").Scan(&col1))
	require.Equal(t, uint64(100_000), col1)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandAsciiString(n int) string {
	return randString(n, letterBytes)
}

var src = rand.NewSource(time.Now().UnixNano())

func randString(n int, bytes string) string {
	sb := strings.Builder{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(bytes) {
			sb.WriteByte(bytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
// thanks to https://golangcode.com/print-the-current-memory-usage/
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
