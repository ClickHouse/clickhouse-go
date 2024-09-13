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
	"context"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/nuonco/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestInterval(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const query = `
		SELECT
			  INTERVAL 1 SECOND
			, INTERVAL 4 SECOND
			, INTERVAL 1 MINUTE
			, INTERVAL 5 MINUTE
		`
	var (
		col1 string
		col2 string
		col3 string
		col4 string
	)
	err = conn.QueryRow(ctx, query).Scan(
		&col1,
		&col2,
		&col3,
		&col4,
	)
	require.NoError(t, err)
	assert.Equal(t, "1 Second", col1)
	assert.Equal(t, "4 Seconds", col2)
	assert.Equal(t, "1 Minute", col3)
	assert.Equal(t, "5 Minutes", col4)
}
