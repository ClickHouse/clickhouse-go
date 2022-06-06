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
	"fmt"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}
func checkMinServerVersion(conn driver.Conn, major, minor, patch uint64) error {
	v, err := conn.ServerVersion()
	if err != nil {
		panic(err)
	}
	if v.Version.Major < major || (v.Version.Major == major && v.Version.Minor < minor) || (v.Version.Major == major && v.Version.Minor == minor && v.Version.Patch < patch) {
		return fmt.Errorf("unsupported server version %d.%d < %d.%d", v.Version.Major, v.Version.Minor, major, minor)
	}
	return nil
}
