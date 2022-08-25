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
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"strconv"
	"strings"
	"time"
)

func GetStdTestEnvironment() (clickhouse_tests.ClickHouseTestEnvironment, error) {
	return clickhouse_tests.GetTestEnvironment("std")
}

func CheckMinServerVersion(conn *sql.DB, major, minor, patch uint64) error {
	var version struct {
		Major uint64
		Minor uint64
		Patch uint64
	}
	var res string
	if err := conn.QueryRow("SELECT version()").Scan(&res); err != nil {
		panic(err)
	}
	for i, v := range strings.Split(res, ".") {
		switch i {
		case 0:
			version.Major, _ = strconv.ParseUint(v, 10, 64)
		case 1:
			version.Minor, _ = strconv.ParseUint(v, 10, 64)
		case 2:
			version.Patch, _ = strconv.ParseUint(v, 10, 64)
		}
	}
	if version.Major < major || (version.Major == major && version.Minor < minor) || (version.Major == major && version.Minor == minor && version.Patch < patch) {
		return fmt.Errorf("unsupported server version %d.%d.%d < %d.%d.%d", version.Major, version.Minor, version.Patch, major, minor, patch)
	}
	return nil
}

func GetDSNConnection(environment string, protocol clickhouse.Protocol, secure bool, compress string) (*sql.DB, error) {
	env, err := clickhouse_tests.GetTestEnvironment(environment)
	if err != nil {
		return nil, err
	}
	switch protocol {
	case clickhouse.HTTP:
		switch secure {
		case true:
			return sql.Open("clickhouse", fmt.Sprintf(fmt.Sprintf("https://%s:%s@%s:%d/%s?secure=true&compress=%s", env.Username, env.Password, env.Host, env.HttpsPort, env.Database, compress)))
		case false:
			return sql.Open("clickhouse", fmt.Sprintf(fmt.Sprintf("http://%s:%s@%s:%d/%s?compress=%s", env.Username, env.Password, env.Host, env.HttpPort, env.Database, compress)))
		}
	case clickhouse.Native:
		switch secure {
		case true:
			return sql.Open("clickhouse", fmt.Sprintf(fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?secure=true&compress=%s&wait_end_of_query=1", env.Username, env.Password, env.Host, env.SslPort, env.Database, compress)))
		case false:
			return sql.Open("clickhouse", fmt.Sprintf(fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?compress=%s&wait_end_of_query=1", env.Username, env.Password, env.Host, env.Port, env.Database, compress)))
		}
	}
	return nil, fmt.Errorf("unsupport protocol - %s", protocol.String())
}

func GetOpenDBConnection(environment string, protocol clickhouse.Protocol, settings clickhouse.Settings, tlsConfig *tls.Config, compression *clickhouse.Compression) (*sql.DB, error) {
	env, err := clickhouse_tests.GetTestEnvironment(environment)
	if err != nil {
		return nil, err
	}
	var port int
	switch protocol {
	case clickhouse.HTTP:
		port = env.HttpPort
		if tlsConfig != nil {
			port = env.HttpsPort
		}
	case clickhouse.Native:
		port = env.Port
		if tlsConfig != nil {
			port = env.SslPort
		}
	}
	if settings == nil {
		settings = clickhouse.Settings{}
	}
	if protocol == clickhouse.HTTP {
		settings["wait_end_of_query"] = 1
	}
	return clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		Settings:    settings,
		DialTimeout: 5 * time.Second,
		Compression: compression,
		TLS:         tlsConfig,
		Protocol:    protocol,
	}), nil
}
