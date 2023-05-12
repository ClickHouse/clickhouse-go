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
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIssue546(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 2000000,
	}),
		clickhouse.WithProgress(func(p *clickhouse.Progress) {
			fmt.Println("progress: ", p)
		}), clickhouse.WithProfileInfo(func(p *clickhouse.ProfileInfo) {
			fmt.Println("profile info: ", p)
		}))
	require.NoError(t, conn.Ping(context.Background()))
	if exception, ok := err.(*clickhouse.Exception); ok {
		fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
	}
	assert.NoError(t, err)

	rows, err := conn.Query(ctx, "SELECT * FROM system.numbers LIMIT 2000000", time.Now())
	assert.NoError(t, err)
	i := 0
	for rows.Next() {
		var (
			col1 uint64
		)
		if err := rows.Scan(&col1); err != nil {
			assert.NoError(t, err)
		}
		i += 1
	}
	assert.NoError(t, rows.Err())
	assert.Equal(t, 2000000, i)
	rows.Close()
}
