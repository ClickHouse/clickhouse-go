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

package tests

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestBatchContextCancellation(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)
	opts.MaxOpenConns = 1
	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, conn.Exec(context.Background(), "create table if not exists test_batch_cancellation (x String) engine=Memory"))
	defer conn.Exec(context.Background(), "drop table if exists test_batch_cancellation")

	b, err := conn.PrepareBatch(ctx, "insert into test_batch_cancellation")
	require.NoError(t, err)
	for i := 0; i < 1_000_000; i++ {
		require.NoError(t, b.Append("value"))
	}

	cancel()

	require.Error(t, b.Send(), context.DeadlineExceeded.Error())

	// assert if connection is properly released after context cancellation
	require.NoError(t, conn.Exec(context.Background(), "SELECT 1"))
}
