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
		name                    string
		query                   string
		expectedNormalizedQuery string
		expectedTableName       string
		expectedColumns         []string
		expectedError           bool
	}{
		{
			name:                    "Regular insert",
			query:                   "INSERT INTO table_name (col1, col2) VALUES (1, 2)",
			expectedNormalizedQuery: "INSERT INTO table_name (col1, col2) FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			name:                    "Lowercase insert",
			query:                   "insert into table_name (col1, col2) values (1, 2)",
			expectedNormalizedQuery: "insert into table_name (col1, col2) FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			name: "Insert with mixed case, multiline and format specified",
			query: `INSERT INTO "db"."table_name" (
						col1,
						col2
					) Values (
						1,
						2
					)
					format JSONEachRow`,
			expectedNormalizedQuery: `INSERT INTO "db"."table_name" (
						col1,
						col2
					) FORMAT Native`,
			expectedTableName: "\"db\".\"table_name\"",
			expectedColumns:   []string{"col1", "col2"},
			expectedError:     false,
		},
		{
			name: "Multiline insert",
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
			name: "Multiline insert, with columns inline",
			query: `INSERT INTO table_name (col1, col2) VALUES (
						1,
						2
					)`,
			expectedNormalizedQuery: `INSERT INTO table_name (col1, col2) FORMAT Native`,
			expectedTableName:       "table_name",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			name: "Multiline insert, with values inline",
			query: `INSERT INTO table_name (
						col1,
						col2
					) VALUES (1, 2)`,
			expectedNormalizedQuery: `INSERT INTO table_name (
						col1,
						col2
					) FORMAT Native`,
			expectedTableName: "table_name",
			expectedColumns:   []string{"col1", "col2"},
			expectedError:     false,
		},
		{
			name:                    "Insert with backtick quoted database and table names",
			query:                   "INSERT INTO `db`.`table_name` (col1, col2) VALUES (1, 2)",
			expectedNormalizedQuery: "INSERT INTO `db`.`table_name` (col1, col2) FORMAT Native",
			expectedTableName:       "`db`.`table_name`",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			name:                    "Insert with double quoted database and table names",
			query:                   "INSERT INTO \"db\".\"table_name\" (col1, col2) VALUES (1, 2)",
			expectedNormalizedQuery: "INSERT INTO \"db\".\"table_name\" (col1, col2) FORMAT Native",
			expectedTableName:       "\"db\".\"table_name\"",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			name:                    "Insert with special characters in database and table names",
			query:                   "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2`",
			expectedNormalizedQuery: "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2` FORMAT Native",
			expectedTableName:       "`_test_1345# $.ДБ`.`2. Таблица №2`",
			expectedColumns:         []string{},
			expectedError:           false,
		},
		{
			name:                    "Insert with special characters in database and table names, with columns",
			query:                   "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2` (col1, col2)",
			expectedNormalizedQuery: "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2` (col1, col2) FORMAT Native",
			expectedTableName:       "`_test_1345# $.ДБ`.`2. Таблица №2`",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			name:                    "Insert with special characters in database and table names, with columns and values",
			query:                   "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2` (col1, col2) VALUES (1, 2)",
			expectedNormalizedQuery: "INSERT INTO `_test_1345# $.ДБ`.`2. Таблица №2` (col1, col2) FORMAT Native",
			expectedTableName:       "`_test_1345# $.ДБ`.`2. Таблица №2`",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			name:                    "Insert without database name",
			query:                   "INSERT INTO table_name (col1, col2) VALUES (1, 2) FORMAT Native",
			expectedNormalizedQuery: "INSERT INTO table_name (col1, col2) FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{"col1", "col2"},
			expectedError:           false,
		},
		{
			name:                    "Insert without columns and values",
			query:                   "INSERT INTO table_name",
			expectedNormalizedQuery: "INSERT INTO table_name FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{},
			expectedError:           false,
		},
		{
			name:                    "Insert with format",
			query:                   "INSERT INTO table_name FORMAT Native",
			expectedNormalizedQuery: "INSERT INTO table_name FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{},
			expectedError:           false,
		},
		{
			name:                    "Insert with lowercase format",
			query:                   "INSERT INTO table_name format Native",
			expectedNormalizedQuery: "INSERT INTO table_name FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{},
			expectedError:           false,
		},
		{
			name:                    "Insert with JSONEachRow format",
			query:                   "INSERT INTO table_name FORMAT JSONEachRow",
			expectedNormalizedQuery: "INSERT INTO table_name FORMAT Native",
			expectedTableName:       "table_name",
			expectedColumns:         []string{},
			expectedError:           false,
		},
		{
			name:                    "Insert with quoted table name only",
			query:                   "INSERT INTO `table_name` VALUES (1, 2)",
			expectedNormalizedQuery: "INSERT INTO `table_name` FORMAT Native",
			expectedTableName:       "`table_name`",
			expectedColumns:         []string{},
			expectedError:           false,
		},
		{
			name:          "Select, should produce error",
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
