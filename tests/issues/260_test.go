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
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIssue260(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, nil)
	)
	require.NoError(t, err)
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE issue_260 (
			Col1 Nullable(DateTime('UTC'))
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_260")
	}()
	err = conn.Exec(ctx, ddl)
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_260")
	require.NoError(t, err)
	require.NoError(t, batch.Append(nil))
	require.NoError(t, batch.Send())
	var col1 *time.Time
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM issue_260").Scan(&col1))
	assert.Nil(t, col1)
}
