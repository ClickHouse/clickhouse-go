package issues

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestStressHTTPBatchConcurrency(t *testing.T) {
	t.Skip("intense test for local debugging")

	env, err := tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	port := env.HttpPort
	var tlsConfig *tls.Config
	if useSSL {
		tlsConfig = &tls.Config{}
		port = env.HttpsPort
	}
	conn, err := tests.GetConnectionWithOptions(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		Debug: true,
		Debugf: func(format string, v ...any) {
			t.Logf(format, v...)
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		TLS:          tlsConfig,
		MaxIdleConns: 10,
		MaxOpenConns: 10, // intense concurrency
	})
	defer conn.Close()

	err = conn.Exec(context.Background(), `CREATE TABLE http_batch_issue (x String) ENGINE Memory`)
	require.NoError(t, err)
	defer func() {
		err := conn.Exec(context.Background(), "DROP TABLE http_batch_issue")
		if err != nil {
			t.Log("failed drop table", err)
		}
	}()

	var totalCount atomic.Int64

	doBatchInsert := func(id, batchID int, conn clickhouse.Conn) error {
		ctx := context.Background()
		t.Log(id, "PreparingBatch")
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO http_batch_issue")
		if err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
		defer func() {
			err := batch.Close()
			if err != nil {
				t.Log("failed batch close", err)
			}
		}()

		for i := 0; i < 5_000; i++ {
			totalCount.Add(1)
			str := strings.Repeat("longlongstring", 100)
			err := batch.Append(fmt.Sprintf("%s=%d;", str, i))
			if err != nil {
				return fmt.Errorf("append: %w", err)
			}
		}

		count := batch.Rows()
		err = batch.Send()
		if err != nil {
			return fmt.Errorf("send: %w", err)
		}

		t.Logf("[%d] inserted %d for batch %d", id, count, batchID)

		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				err := doBatchInsert(i, j, conn)
				if err != nil {
					t.Fatal("index", i, "failed insert for batch", j, err)
				}
			}
		}()
	}

	wg.Wait()

	t.Log("Total rows:", totalCount.Load())
}
