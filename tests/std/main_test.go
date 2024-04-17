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
	"crypto/tls"
	"database/sql"
	"net/url"
	"os"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

const testSet string = "std"

func TestMain(m *testing.M) {
	os.Exit(clickhouse_tests.Runtime(m, testSet))
}

func GetStdDSNConnection(protocol clickhouse.Protocol, secure bool, opts url.Values) (*sql.DB, error) {
	return GetDSNConnection(testSet, protocol, secure, opts)
}

func GetStdOpenDBConnection(protocol clickhouse.Protocol, settings clickhouse.Settings, tlsConfig *tls.Config, compression *clickhouse.Compression) (*sql.DB, error) {
	return GetOpenDBConnection(testSet, protocol, settings, tlsConfig, compression)
}
