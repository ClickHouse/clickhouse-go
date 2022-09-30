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
	"encoding/json"
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
	var res string
	if err := conn.QueryRow("SELECT version()").Scan(&res); err != nil {
		panic(err)
	}
	var version clickhouse_tests.Version
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
	return clickhouse_tests.CheckMinVersion(clickhouse_tests.Version{
		Major: major,
		Minor: minor,
		Patch: patch,
	}, version)
}

func GetDSNConnection(environment string, protocol clickhouse.Protocol, secure bool, compress string) (*sql.DB, error) {
	env, err := clickhouse_tests.GetTestEnvironment(environment)
	enforceReplication := ""
	if clickhouse_tests.CheckMinVersion(clickhouse_tests.Version{
		Major: 22,
		Minor: 8,
		Patch: 0,
	}, env.Version) == nil {
		enforceReplication = "database_replicated_enforce_synchronous_settings=1"
	}
	if err != nil {
		return nil, err
	}
	insertQuorum := clickhouse_tests.GetEnv("CLICKHOUSE_QUORUM_INSERT", "1")
	switch protocol {
	case clickhouse.HTTP:
		switch secure {
		case true:
			return sql.Open("clickhouse", fmt.Sprintf(fmt.Sprintf("https://%s:%s@%s:%d/%s?%s&secure=true&compress=%s&wait_end_of_query=1&insert_quorum=%s&insert_quorum_parallel=0&select_sequential_consistency=1", env.Username, env.Password, env.Host, env.HttpsPort, env.Database, enforceReplication, compress, insertQuorum)))
		case false:
			return sql.Open("clickhouse", fmt.Sprintf(fmt.Sprintf("http://%s:%s@%s:%d/%s?%s&compress=%s&wait_end_of_query=1&insert_quorum=%s&insert_quorum_parallel=0&select_sequential_consistency=1", env.Username, env.Password, env.Host, env.HttpPort, env.Database, enforceReplication, compress, insertQuorum)))
		}
	case clickhouse.Native:
		switch secure {
		case true:
			return sql.Open("clickhouse", fmt.Sprintf(fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?%s&secure=true&compress=%s&insert_quorum=%s&insert_quorum_parallel=0&select_sequential_consistency=1", env.Username, env.Password, env.Host, env.SslPort, env.Database, enforceReplication, compress, insertQuorum)))
		case false:
			return sql.Open("clickhouse", fmt.Sprintf(fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?%s&compress=%s&insert_quorum=%s&insert_quorum_parallel=0&select_sequential_consistency=1", env.Username, env.Password, env.Host, env.Port, env.Database, enforceReplication, compress, insertQuorum)))
		}
	}
	return nil, fmt.Errorf("unsupport protocol - %s", protocol.String())
}

func GetConnectionFromDSN(dsn string) (*sql.DB, error) {
	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return conn, err
	}
	if CheckMinServerVersion(conn, 22, 8, 0) == nil {
		dsn = fmt.Sprintf("%s&database_replicated_enforce_synchronous_settings=1", dsn)
	}
	insertQuorum := clickhouse_tests.GetEnv("CLICKHOUSE_QUORUM_INSERT", "1")
	dsn = fmt.Sprintf("%s&insert_quorum=%s&insert_quorum_parallel=0&select_sequential_consistency=1", dsn, insertQuorum)
	if strings.HasPrefix(dsn, "http") {
		dsn = fmt.Sprintf("%s&wait_end_of_query=1", dsn)
	}
	return sql.Open("clickhouse", dsn)
}

func GetConnectionWithOptions(options *clickhouse.Options) *sql.DB {
	if options.Settings == nil {
		options.Settings = clickhouse.Settings{}
	}
	conn := clickhouse.OpenDB(options)
	if CheckMinServerVersion(conn, 22, 8, 0) == nil {
		options.Settings["database_replicated_enforce_synchronous_settings"] = "1"
	}
	var err error
	options.Settings["insert_quorum"], err = strconv.Atoi(clickhouse_tests.GetEnv("CLICKHOUSE_QUORUM_INSERT", "1"))
	options.Settings["insert_quorum_parallel"] = 0
	options.Settings["select_sequential_consistency"] = 1
	if err != nil {
		return nil
	}
	return clickhouse.OpenDB(options)
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
	settings["insert_quorum"], err = strconv.Atoi(clickhouse_tests.GetEnv("CLICKHOUSE_QUORUM_INSERT", "1"))
	settings["insert_quorum_parallel"] = 0
	settings["select_sequential_consistency"] = 1
	if clickhouse_tests.CheckMinVersion(clickhouse_tests.Version{
		Major: 22,
		Minor: 8,
		Patch: 0,
	}, env.Version) == nil {
		settings["database_replicated_enforce_synchronous_settings"] = "1"
	}
	if err != nil {
		return nil, err
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

func ToJson(obj interface{}) string {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "unable to marshal"
	}
	return string(bytes)
}
