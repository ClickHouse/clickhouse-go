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

package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractNormalizedInsertQueryAndColumns(t *testing.T) {
	var testCases = []struct {
		query                   string
		expectedNormalizedQuery string
		expectedTableName       string
		expectedColumns         []string
		expectedError           bool
	}{
		{
			query:                   "INSERT INTO table_name (col1, col2) VALUES (1, 2)",
			expectedNormalizedQuery: "INSERT INTO table_name (col1, col2) FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			query: `INSERT INTO table_name (
						col1,
						col2
					) VALUES (
						1,
						2
					)`,
			expectedNormalizedQuery: `INSERT INTO table_name (
						col1,
						col2
					) FORMAT Native`,
			expectedTableName: "table_name",
			expectedColumns:   []string{"col1", "col2"},
			expectedError:     false,
		},
		{
			query:                   "INSERT INTO `db`.`table_name` (col1, col2) VALUES (1, 2)",
			expectedNormalizedQuery: "INSERT INTO `db`.`table_name` (col1, col2) FORMAT Native",
			expectedTableName:       "`db`.`table_name`",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			query:                   "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2`",
			expectedNormalizedQuery: "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2` FORMAT Native",
			expectedTableName:       "`_test_1345# $.ДБ`.`2. Таблица №2`",
			expectedColumns:         []string{},
			expectedError:           false,
		},
		{
			query:                   "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2` (col1, col2)",
			expectedNormalizedQuery: "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2` (col1, col2) FORMAT Native",
			expectedTableName:       "`_test_1345# $.ДБ`.`2. Таблица №2`",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			query:                   "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2` (col1, col2) VALUES (1, 2)",
			expectedNormalizedQuery: "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2` (col1, col2) FORMAT Native",
			expectedTableName:       "`_test_1345# $.ДБ`.`2. Таблица №2`",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			query:                   "INSERT INTO table_name (col1, col2) VALUES (1, 2) FORMAT Native",
			expectedNormalizedQuery: "INSERT INTO table_name (col1, col2) FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			query:                   "INSERT INTO table_name",
			expectedNormalizedQuery: "INSERT INTO table_name FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{},
			expectedError:           false,
		},
		{
			query:                   "INSERT INTO table_name FORMAT Native",
			expectedNormalizedQuery: "INSERT INTO table_name FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{},
			expectedError:           false,
		},
		{
			query:                   "INSERT INTO table_name FORMAT JSONEachRow",
			expectedNormalizedQuery: "INSERT INTO table_name FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{},
			expectedError:           false,
		},
		{
			query:                   "INSERT INTO `table_name` VALUES (1, 2)",
			expectedNormalizedQuery: "INSERT INTO `table_name` FORMAT Native",
			expectedTableName:       "`table_name`",
			expectedColumns:         []string{},
			expectedError:           false,
		},
		{
			query:         "SELECT * FROM table_name",
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.query, func(t *testing.T) {
			normalizedQuery, tableName, columns, err := extractNormalizedInsertQueryAndColumns(tc.query)
			if tc.expectedError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expectedNormalizedQuery, normalizedQuery)
			assert.Equal(t, tc.expectedTableName, tableName)
			assert.Equal(t, tc.expectedColumns, columns)
		})
	}
}
