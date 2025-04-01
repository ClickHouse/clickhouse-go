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

package std

import (
	"crypto/rand"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type BinFixedString struct {
	data [10]byte
}

func (bin *BinFixedString) MarshalBinary() ([]byte, error) {
	return bin.data[:], nil
}

func (bin *BinFixedString) UnmarshalBinary(b []byte) error {
	copy(bin.data[:], b)
	return nil
}

func (bin *BinFixedString) Scan(src any) error {
	return bin.UnmarshalBinary([]byte(src.(string)))
}

func TestStdFixedString(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			const ddl = `
				CREATE TABLE test_std_fixed_string (
						Col1 FixedString(10)
					, Col2 FixedString(10)
					, Col3 Nullable(FixedString(10))
					, Col4 Array(FixedString(10))
					, Col5 Array(Nullable(FixedString(10)))
				) Engine MergeTree() ORDER BY tuple()
			`
			defer func() {
				conn.Exec("DROP TABLE test_std_fixed_string")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_std_fixed_string")
			require.NoError(t, err)
			var (
				col1Data = "ClickHouse"
				col2Data = &BinFixedString{}
				col3Data = &col1Data
				col4Data = []string{"ClickHouse", "ClickHouse", "ClickHouse"}
				col5Data = []*string{&col1Data, nil, &col1Data}
			)
			_, err = rand.Read(col2Data.data[:])
			require.NoError(t, err)
			_, err = batch.Exec(col1Data, col2Data, col3Data, col4Data, col5Data)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				col1 string
				col2 BinFixedString
				col3 *string
				col4 []string
				col5 []*string
			)
			require.NoError(t, conn.QueryRow("SELECT * FROM test_std_fixed_string").Scan(&col1, &col2, &col3, &col4, &col5))
			assert.Equal(t, col1Data, col1)
			assert.Equal(t, col2Data.data, col2.data)
			assert.Equal(t, col3Data, col3)
			assert.Equal(t, col4Data, col4)
			assert.Equal(t, col5Data, col5)
		})
	}
}
