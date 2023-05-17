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

package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test957(t *testing.T) {
	// given
	ctx := context.Background()
	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	// when the client is configured to use the test environment
	opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, clickhouse.Settings{})
	// and the client is configured to have only 1 connection
	opts.MaxIdleConns = 2
	opts.MaxOpenConns = 1
	// and the client is configured to have a connection lifetime of 1/10 of a second
	opts.ConnMaxLifetime = time.Second / 10
	conn, err := clickhouse.Open(&opts)
	require.NoError(t, err)

	// then the client should be able to execute queries for 1 second
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rows, err := conn.Query(ctx, "SELECT 1")
		require.NoError(t, err)
		rows.Close()
	}
}
