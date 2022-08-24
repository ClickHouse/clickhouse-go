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
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"os"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	useDocker := strings.ToLower(clickhouse_tests.GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if useDocker == "false" {
		fmt.Printf("Using external ClickHouse for std IT tests -  %s:%s\n",
			clickhouse_tests.GetEnv("CLICKHOUSE_PORT", "9000"),
			clickhouse_tests.GetEnv("CLICKHOUSE_HOST", "localhost"))
		env, err := clickhouse_tests.GetExternalTestEnvironment()
		if err != nil {
			panic(err)
		}
		clickhouse_tests.SetTestEnvironment("std", env)
		os.Exit(m.Run())
	}
	testEnv, err := clickhouse_tests.CreateClickHouseTestEnvironment("std")
	if err != nil {
		panic(err)
	}
	defer testEnv.Container.Terminate(context.Background()) //nolint
	os.Exit(m.Run())
}
