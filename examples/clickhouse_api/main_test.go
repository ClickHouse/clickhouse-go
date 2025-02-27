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

package clickhouse_api

import (
	"context"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"testing"
)

func TestMain(m *testing.M) {
	ResetRandSeed()
	fmt.Printf("using random seed %d for %s tests\n", randSeed, TestSet)

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

// ClickHouse API tests

func TestOpenTelemetry(t *testing.T) {
	require.NoError(t, OpenTelemetry())
}

func TestTuples(t *testing.T) {
	require.NoError(t, TupleInsertRead())
}

func TestAppendStruct(t *testing.T) {
	require.NoError(t, AppendStruct())
}

func TestArrayInsertRead(t *testing.T) {
	require.NoError(t, ArrayInsertRead())
}

func TestAsyncInsert(t *testing.T) {
	require.NoError(t, AsyncInsert())
}

func TestBatchInsert(t *testing.T) {
	require.NoError(t, BatchInsert())
}

func TestBatchWithReleaseConnection(t *testing.T) {
	require.NoError(t, BatchWithReleaseConnection())
}

func TestAuthConnect(t *testing.T) {
	require.NoError(t, Auth())
}

func TestBigInt(t *testing.T) {
	require.NoError(t, ReadWriteBigInt())
}

func TestBind(t *testing.T) {
	require.NoError(t, BindParameters())
}

func TestSpecialCaseBind(t *testing.T) {
	require.NoError(t, SpecialBind())
}

func TestColumnInsert(t *testing.T) {
	require.NoError(t, ColumnInsert())
}

func TestConnect(t *testing.T) {
	require.NoError(t, Connect())
}

func TestCompression(t *testing.T) {
	require.NoError(t, Compress())
}

func TestConnectWithSettings(t *testing.T) {
	require.NoError(t, PingWithSettings())
}

func TestDecimal(t *testing.T) {
	require.NoError(t, ReadWriteDecimal())
}

func TestContext(t *testing.T) {
	require.NoError(t, UseContext())
}

func TestCustomTypes(t *testing.T) {
	require.NoError(t, CustomTypes())
}

func TestDynamicScan(t *testing.T) {
	require.NoError(t, DynamicScan())
}

func TestExternalTable(t *testing.T) {
	require.NoError(t, ExternalData())
}

func TestExec(t *testing.T) {
	require.NoError(t, Exec())
}

func TestGeo(t *testing.T) {
	require.NoError(t, GeoInsertRead())
}

func TestMapInsertRead(t *testing.T) {
	require.NoError(t, MapInsertRead())
}

func TestIterableOrderedMapInsertRead(t *testing.T) {
	require.NoError(t, IterableOrderedMapInsertRead())
}

func TestMultiHostConnect(t *testing.T) {
	require.NoError(t, MultiHostVersion())
	require.NoError(t, MultiHostRoundRobinVersion())
	require.NoError(t, MultiHostRandomVersion())
}

func TestNested(t *testing.T) {
	require.NoError(t, NestedUnFlattened())
	require.NoError(t, NestedFlattened())
}

func TestProgress(t *testing.T) {
	require.NoError(t, ProgressProfileLogs())
}

func TestScanStruct(t *testing.T) {
	require.NoError(t, ScanStruct())
}

func TestQueryRow(t *testing.T) {
	require.NoError(t, QueryRow())
}

func TestQueryWithParameters(t *testing.T) {
	require.NoError(t, QueryWithParameters())
}

func TestSelectStruct(t *testing.T) {
	require.NoError(t, SelectStruct())
}

func TestTypeConvert(t *testing.T) {
	require.NoError(t, ConvertedInsert())
}

func TestUUID(t *testing.T) {
	require.NoError(t, UUIDInsertRead())
}

func TestNullable(t *testing.T) {
	require.NoError(t, NullableInsertRead())
}

func TestQueryRows(t *testing.T) {
	require.NoError(t, QueryRows())
}

func TestSSL(t *testing.T) {
	require.NoError(t, SSLVersion())
}

func TestSSLNoVerify(t *testing.T) {
	require.NoError(t, SSLNoVerifyVersion())
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

func TestJSONStructExample(t *testing.T) {
	clickhouse_tests.SkipOnCloud(t, "cannot modify JSON settings on cloud")
	require.NoError(t, JSONStructExample())
}

func TestJSONFastStructExample(t *testing.T) {
	clickhouse_tests.SkipOnCloud(t, "cannot modify JSON settings on cloud")
	require.NoError(t, JSONFastStructExample())
}

func TestJSONStringExample(t *testing.T) {
	clickhouse_tests.SkipOnCloud(t, "cannot modify JSON settings on cloud")
	t.Skip("client cannot receive JSON strings")
	require.NoError(t, JSONStringExample())
}
