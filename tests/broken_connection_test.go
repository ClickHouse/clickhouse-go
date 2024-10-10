package tests

import (
	"context"
	"errors"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"os"
	"syscall"
	"testing"
)

//goland:noinspection ALL
const insertQry = "INSERT INTO test (foo, foo2)"

func TestBatchFlushBrokenConn(t *testing.T) {
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	require.NotNil(t, env)
	ctx := context.Background()
	client, err := testcontainers.NewDockerClientWithOpts(ctx)
	require.NoError(t, err)
	chClient, err := getConnection(env, env.Database, clickhouse.Settings{}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})

	err = chClient.Exec(ctx, "CREATE TABLE test (foo String, foo2 String)  ENGINE = MergeTree ORDER BY (foo)")
	require.NoError(t, err)
	batch, err := chClient.PrepareBatch(ctx, insertQry, driver.WithCloseOnFlush())
	require.NoError(t, err)
	err = batch.Append("bar", "bar")
	require.NoError(t, err)
	err = batch.Flush()
	require.NoError(t, err)
	err = batch.Append("bar2", "bar2")
	require.NoError(t, err)
	err = batch.Flush()
	require.NoError(t, err)

	err = batch.Append(RandAsciiString(200000000), RandAsciiString(20000000))

	require.NoError(t, err)
	ch := make(chan struct{})
	go func() {
		err = batch.Flush()
		close(ch)
	}()
	//timeout := 0
	err2 := client.ContainerKill(ctx, env.ContainerID, "KILL")
	<-ch
	require.NoError(t, err2)
	require.True(t, errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET))
	err = client.ContainerStart(ctx, env.ContainerID, container.StartOptions{})
	require.NoError(t, err)
	err = batch.Flush()
	// retry after server is up should have either no error, or a reconnect error (for example because the mapped port
	// changed on container startup)
	require.True(t, err == nil || errors.Is(err, syscall.ECONNREFUSED) || os.IsTimeout(err), err)

}
