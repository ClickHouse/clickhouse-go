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
	"fmt"
	"github.com/stretchr/testify/assert"
	"runtime"
	"testing"
)

func TestClientInfoString(t *testing.T) {
	// e.g. clickhouse-go/2.5.1
	expectedClientProduct := fmt.Sprintf("%s/%d.%d.%d", ClientName, ClientVersionMajor, ClientVersionMinor, ClientVersionPatch)

	// e.g. lv:go/1.19.5; os:darwin
	expectedDefaultMeta := fmt.Sprintf("lv:go/%s; os:%s", runtime.Version()[2:], runtime.GOOS)

	testCases := map[string]struct {
		actual   ClientInfo
		expected string
	}{
		"client": {
			ClientInfo{},
			// e.g. clickhouse-go/2.5.1 (lv:go/1.19.5; os:darwin)
			fmt.Sprintf("%s (%s)", expectedClientProduct, expectedDefaultMeta),
		},
		"client with comment": {
			ClientInfo{
				comment: []string{"database/sql"},
			},
			// e.g. clickhouse-go/2.5.1 (database/sql; lv:go/1.19.5; os:darwin)
			fmt.Sprintf("%s (database/sql; %s)", expectedClientProduct, expectedDefaultMeta),
		},
		"additional product": {
			ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "grafana-datasource", Version: "0.1.1"},
				},
			},
			// e.g. grafana-datasource/0.1.1 clickhouse-go/2.5.1 (lv:go/1.19.5; os:darwin)
			fmt.Sprintf("grafana-datasource/0.1.1 %s (%s)", expectedClientProduct, expectedDefaultMeta),
		},
		"additional products with comment": {
			ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "grafana", Version: "6.1"},
					{Name: "grafana-datasource", Version: "0.1.1"},
				},
				comment: []string{"database/sql"},
			},
			// e.g. grafana/6.1 grafana-datasource/0.1.1 clickhouse-go/2.5.1 (database/sql; lv:go/1.19.5; os:darwin)
			fmt.Sprintf("grafana/6.1 grafana-datasource/0.1.1 %s (database/sql; %s)", expectedClientProduct, expectedDefaultMeta),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.actual.String()

			assert.Equal(t, testCase.expected, actual)
		})
	}
}
