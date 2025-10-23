package std

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func TestMain(m *testing.M) {
	seed := time.Now().UnixNano()
	fmt.Printf("using random seed %d for %s tests\n", seed, TestSet)
	rand.Seed(seed)
	useDocker, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if err != nil {
		panic(err)
	}
	var env clickhouse_tests.ClickHouseTestEnvironment
	switch useDocker {
	case true:
		env, err = clickhouse_tests.CreateClickHouseTestEnvironment(TestSet)
		if err != nil {
			panic(err)
		}
		defer env.Container.Terminate(context.Background()) //nolint
	case false:
		fmt.Printf("skipping %s tests as docker only\n", TestSet)
		os.Exit(0)
	}
	clickhouse_tests.SetTestEnvironment(TestSet, env)
	if err := clickhouse_tests.CreateDatabase(TestSet); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// Std Tests

func TestStdConnect(t *testing.T) {
	require.NoError(t, Connect())
	require.NoError(t, ConnectDSN())
}

func TestStdHTTPConnect(t *testing.T) {
	require.NoError(t, ConnectHTTP())
	require.NoError(t, ConnectDSNHTTP())
}

func TestStdConnectSSL(t *testing.T) {
	require.NoError(t, ConnectSSL())
	require.NoError(t, ConnectDSNSSL())
}

func TestStdAuth(t *testing.T) {
	require.NoError(t, ConnectAuth())
	require.NoError(t, ConnectDSNAuth())
}

func TestStdMultiHost(t *testing.T) {
	require.NoError(t, MultiStdHost())
	require.NoError(t, MultiStdHostDSN())
}

func TestStdExec(t *testing.T) {
	require.NoError(t, Exec())
}

func TestStdBatch(t *testing.T) {
	require.NoError(t, BatchInsert())
}

func TestStdQueryRow(t *testing.T) {
	require.NoError(t, QueryRow())
}

func TestStdQueryRows(t *testing.T) {
	require.NoError(t, QueryRows())
}

func TestStdQueryWithParameters(t *testing.T) {
	require.NoError(t, QueryWithParameters())
}

func TestStdAsyncInsert(t *testing.T) {
	require.NoError(t, AsyncInsertNative())
	require.NoError(t, AsyncInsertNative_WithPrepare())
	require.NoError(t, AsyncInsertHTTP())
	require.NoError(t, AsyncInsertHTTP_WithPrepare())
}

func TestStdMapInsertRead(t *testing.T) {
	require.NoError(t, MapInsertRead())
}

func TestStdCompression(t *testing.T) {
	require.NoError(t, CompressOpenDB())
	require.NoError(t, CompressOpen())
}

func TestStdBind(t *testing.T) {
	require.NoError(t, BindParameters())
}

func TestStdContext(t *testing.T) {
	require.NoError(t, UseContext())
}

func TestStdProgress(t *testing.T) {
	require.NoError(t, ProgressProfileLogs())
}

func TestStdDynamicScan(t *testing.T) {
	require.NoError(t, DynamicScan())
}

func TestStdExternalTable(t *testing.T) {
	require.NoError(t, ExternalData())
}

func TestStdOpenTelemetry(t *testing.T) {
	require.NoError(t, OpenTelemetry())
}

func TestOpenDb(t *testing.T) {
	require.NoError(t, OpenDb())
}

func TestConnectionSettings(t *testing.T) {
	require.NoError(t, ConnectSettings())
}

func TestVariantExample(t *testing.T) {
	clickhouse_tests.SkipOnCloud(t, "cannot modify Variant settings on cloud")
	require.NoError(t, VariantExample())
}

func TestDynamicExample(t *testing.T) {
	clickhouse_tests.SkipOnCloud(t, "cannot modify Dynamic settings on cloud")
	require.NoError(t, DynamicExample())
}

func TestJSONPathsExample(t *testing.T) {
	clickhouse_tests.SkipOnCloud(t, "cannot modify JSON settings on cloud")
	require.NoError(t, JSONPathsExample())
}

func TestJSONStringExample(t *testing.T) {
	clickhouse_tests.SkipOnCloud(t, "cannot modify JSON settings on cloud")
	require.NoError(t, JSONStringExample())
}

func TestEndOfProcessAndGotBlock(t *testing.T) {
	require.NoError(t, EndOfProcessAndGotData())
}
