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
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

const testSet string = "native"

func TestMain(m *testing.M) {
	seed := time.Now().UnixNano()
	fmt.Printf("using random seed %d for %s tests\n", seed, testSet)
	rand.Seed(seed)
	useDocker, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if err != nil {
		panic(err)
	}
	var env ClickHouseTestEnvironment
	switch useDocker {
	case true:
		env, err = CreateClickHouseTestEnvironment(testSet)
		if err != nil {
			panic(err)
		}
		defer env.Container.Terminate(context.Background()) //nolint
	case false:
		env, err = GetExternalTestEnvironment(testSet)
		if err != nil {
			panic(err)
		}
	}
	SetTestEnvironment(testSet, env)
	if err := CreateDatabase(testSet); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func GetNativeTestEnvironment() (ClickHouseTestEnvironment, error) {
	return GetTestEnvironment(testSet)
}

func GetNativeConnection(settings clickhouse.Settings, tlsConfig *tls.Config, compression *clickhouse.Compression) (driver.Conn, error) {
	return GetConnection(testSet, settings, tlsConfig, compression)
}
