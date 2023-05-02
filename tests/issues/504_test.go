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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIssue504(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	require.NoError(t, err)
	var result []struct {
		Col1 string
		Col2 uint64
	}
	const query = `
		SELECT *
		FROM
		(
			SELECT
				'A'    AS Col1,
				number AS Col2
			FROM
			(
				SELECT number
				FROM system.numbers
				LIMIT 5
			)
		)
		WHERE (Col1, Col2) IN (@GS)
		`
	err = conn.Select(ctx, &result, query, clickhouse.Named("GS", []clickhouse.GroupSet{
		{Value: []any{"A", 2}},
		{Value: []any{"A", 4}},
	}))
	require.NoError(t, err)
	assert.Equal(t, []struct {
		Col1 string
		Col2 uint64
	}{
		{
			Col1: "A",
			Col2: 2,
		},
		{
			Col1: "A",
			Col2: 4,
		},
	}, result)
}
