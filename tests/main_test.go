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
	"fmt"
	"os"
	"strconv"
	"testing"
)

func TestMain(m *testing.M) {
	useDocker, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if !useDocker {
		fmt.Printf("Using external ClickHouse for native IT tests -  %s:%s\n",
			GetEnv("CLICKHOUSE_PORT", "9000"),
			GetEnv("CLICKHOUSE_HOST", "localhost"))
		env, err := GetExternalTestEnvironment()
		if err != nil {
			panic(err)
		}
		SetTestEnvironment("native", env)
		os.Exit(m.Run())
	}
	testEnv, err := CreateClickHouseTestEnvironment("native")
	if err != nil {
		panic(err)
	}
	defer testEnv.Container.Terminate(context.Background()) //nolint
	os.Exit(m.Run())
}
