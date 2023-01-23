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
	expectedClientProduct := fmt.Sprintf("%s/%d.%d.%d", ClientName, ClientVersionMajor, ClientVersionMinor, ClientVersionPatch)
	expectedDefaultMeta := fmt.Sprintf("lv:go/%s; os:%s", runtime.Version()[2:], runtime.GOOS)

	testCases := map[string]struct {
		actual   ClientInfo
		expected string
	}{
		"client": {
			ClientInfo{},
			fmt.Sprintf("%s (%s)", expectedClientProduct, expectedDefaultMeta),
		},
		"client with meta": {
			ClientInfo{
				Meta: map[string]string{
					"property": "value",
				},
			},
			fmt.Sprintf("%s (%s; property:value)", expectedClientProduct, expectedDefaultMeta),
		},
		"client with multiple meta": {
			ClientInfo{
				Meta: map[string]string{
					"property":  "value",
					"property2": "value",
				},
			},
			fmt.Sprintf("%s (%s; property:value; property2:value)", expectedClientProduct, expectedDefaultMeta),
		},
		"client with multiple meta and comment": {
			ClientInfo{
				Comment: []string{"comment value"},
				Meta: map[string]string{
					"property":  "value",
					"property2": "value",
				},
			},
			fmt.Sprintf("%s (comment value; %s; property:value; property2:value)", expectedClientProduct, expectedDefaultMeta),
		},
		"additional product with multiple meta and comment": {
			ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "grafana-datasource", Version: "0.1.1"},
				},
				Comment: []string{"comment value"},
				Meta: map[string]string{
					"property":  "value",
					"property2": "value",
				},
			},
			fmt.Sprintf("grafana-datasource/0.1.1 %s (comment value; %s; property:value; property2:value)", expectedClientProduct, expectedDefaultMeta),
		},
		"additional products with multiple meta and comment": {
			ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "grafana", Version: "6.1"},
					{Name: "grafana-datasource", Version: "0.1.1"},
				},
				Comment: []string{"comment value"},
				Meta: map[string]string{
					"property":  "value",
					"property2": "value",
				},
			},
			fmt.Sprintf("grafana/6.1 grafana-datasource/0.1.1 %s (comment value; %s; property:value; property2:value)", expectedClientProduct, expectedDefaultMeta),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.actual.String()

			assert.Equal(t, testCase.expected, actual)
		})
	}
}
