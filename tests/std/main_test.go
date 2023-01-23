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
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"
)

const testSet string = "std"

func TestMain(m *testing.M) {
	seed := time.Now().UnixNano()
	fmt.Printf("using random seed %d for %s tests\n", seed, testSet)
	rand.Seed(seed)
	useDocker, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if err != nil {
		panic(err)
	}
	var env clickhouse_tests.ClickHouseTestEnvironment
	switch useDocker {
	case true:
		env, err = clickhouse_tests.CreateClickHouseTestEnvironment(testSet)
		if err != nil {
			panic(err)
		}
		defer env.Container.Terminate(context.Background()) //nolint
	case false:
		env, err = clickhouse_tests.GetExternalTestEnvironment(testSet)
		if err != nil {
			panic(err)
		}
	}
	clickhouse_tests.SetTestEnvironment(testSet, env)
	if err := clickhouse_tests.CreateDatabase(testSet); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func GetStdDSNConnection(protocol clickhouse.Protocol, secure bool, opts url.Values) (*sql.DB, error) {
	return GetDSNConnection(testSet, protocol, secure, opts)
}

func GetStdOpenDBConnection(protocol clickhouse.Protocol, settings clickhouse.Settings, tlsConfig *tls.Config, compression *clickhouse.Compression) (*sql.DB, error) {
	return GetOpenDBConnection(testSet, protocol, settings, tlsConfig, compression)
}
