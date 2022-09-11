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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/rnbondarenko/clickhouse-go/v2"
	"github.com/rnbondarenko/clickhouse-go/v2/lib/driver"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"math/rand"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var testUUID = uuid.NewString()[0:12]
var testTimestamp = time.Now().UnixMilli()

func CheckMinServerVersion(conn driver.Conn, major, minor, patch uint64) error {
	v, err := conn.ServerVersion()
	if err != nil {
		panic(err)
	}
	if v.Version.Major < major || (v.Version.Major == major && v.Version.Minor < minor) || (v.Version.Major == major && v.Version.Minor == minor && v.Version.Patch < patch) {
		return fmt.Errorf("unsupported server version %d.%d < %d.%d", v.Version.Major, v.Version.Minor, major, minor)
	}
	return nil
}

const defaultClickHouseVersion = "latest"

func GetClickHouseTestVersion() string {
	return GetEnv("CLICKHOUSE_VERSION", defaultClickHouseVersion)
}

type ClickHouseTestEnvironment struct {
	Port      int
	HttpPort  int
	SslPort   int
	HttpsPort int
	Host      string
	Username  string
	Password  string
	Database  string
	Container testcontainers.Container `json:"-"`
}

func CreateClickHouseTestEnvironment(testSet string) (ClickHouseTestEnvironment, error) {
	// create a ClickHouse Container
	ctx := context.Background()
	// attempt use docker for CI
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Printf("Docker is not running and no clickhouse connections details were provided. Skipping tests: %s\n", err)
		os.Exit(0)
	}
	err = provider.Health(ctx)
	if err != nil {
		fmt.Printf("Docker is not running and no clickhouse connections details were provided. Skipping IT tests: %s\n", err)
		os.Exit(0)
	}
	fmt.Printf("Using Docker for IT tests\n")
	_, b, _, _ := runtime.Caller(0)
	basePath := filepath.Dir(b)
	if err != nil {
		// can't test without Container
		panic(err)
	}

	expected := []*units.Ulimit{
		{
			Name: "nofile",
			Hard: 262144,
			Soft: 262144,
		},
	}
	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("clickhouse/clickhouse-server:%s", GetClickHouseTestVersion()),
		Name:         fmt.Sprintf("clickhouse-go-%s-%d", strings.ToLower(testSet), time.Now().UnixNano()),
		ExposedPorts: []string{"9000/tcp", "8123/tcp", "9440/tcp", "8443/tcp"},
		WaitingFor:   wait.ForLog("Ready for connections"),
		Mounts: []testcontainers.ContainerMount{
			testcontainers.BindMount(path.Join(basePath, "./resources/custom.xml"), "/etc/clickhouse-server/config.d/custom.xml"),
			testcontainers.BindMount(path.Join(basePath, "./resources/admin.xml"), "/etc/clickhouse-server/users.d/admin.xml"),
			testcontainers.BindMount(path.Join(basePath, "./resources/clickhouse.crt"), "/etc/clickhouse-server/certs/clickhouse.crt"),
			testcontainers.BindMount(path.Join(basePath, "./resources/clickhouse.key"), "/etc/clickhouse-server/certs/clickhouse.key"),
			testcontainers.BindMount(path.Join(basePath, "./resources/CAroot.crt"), "/etc/clickhouse-server/certs/CAroot.crt"),
		},
		Resources: container.Resources{
			Ulimits: expected,
		},
	}
	clickhouseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return ClickHouseTestEnvironment{}, err
	}
	p, _ := clickhouseContainer.MappedPort(ctx, "9000")
	hp, _ := clickhouseContainer.MappedPort(ctx, "8123")
	sslPort, _ := clickhouseContainer.MappedPort(ctx, "9440")
	hps, _ := clickhouseContainer.MappedPort(ctx, "8443")
	testEnv := ClickHouseTestEnvironment{
		Port:      p.Int(),
		HttpPort:  hp.Int(),
		SslPort:   sslPort.Int(),
		HttpsPort: hps.Int(),
		Host:      "localhost",
		// we set this explicitly - note its also set in the /etc/clickhouse-server/users.d/admin.xml
		Username:  "default",
		Password:  "ClickHouse",
		Container: clickhouseContainer,
		Database:  GetEnv("CLICKHOUSE_DATABASE", getDatabaseName(testSet)),
	}
	return testEnv, nil
}

func SetTestEnvironment(testSet string, environment ClickHouseTestEnvironment) {
	bytes, err := json.Marshal(environment)
	if err != nil {
		panic(err)
	}
	os.Setenv(fmt.Sprintf("CLICKHOUSE_%s_ENV", strings.ToUpper(testSet)), string(bytes))
}

func GetTestEnvironment(testSet string) (ClickHouseTestEnvironment, error) {
	useDocker, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if err != nil {
		return ClickHouseTestEnvironment{}, err
	}
	if !useDocker {
		return GetExternalTestEnvironment(testSet)
	}
	sEnv := os.Getenv(fmt.Sprintf("CLICKHOUSE_%s_ENV", strings.ToUpper(testSet)))
	if sEnv == "" {
		return ClickHouseTestEnvironment{}, errors.New("unable to find environment")
	}
	var env ClickHouseTestEnvironment
	if err := json.Unmarshal([]byte(sEnv), &env); err != nil {
		return ClickHouseTestEnvironment{}, err
	}
	return env, nil
}

func GetExternalTestEnvironment(testSet string) (ClickHouseTestEnvironment, error) {
	port, err := strconv.Atoi(GetEnv("CLICKHOUSE_PORT", "9000"))
	if err != nil {
		return ClickHouseTestEnvironment{}, nil
	}
	httpPort, err := strconv.Atoi(GetEnv("CLICKHOUSE_HTTP_PORT", "8123"))
	if err != nil {
		return ClickHouseTestEnvironment{}, nil
	}
	sslPort, err := strconv.Atoi(GetEnv("CLICKHOUSE_SSL_PORT", "9440"))
	if err != nil {
		return ClickHouseTestEnvironment{}, nil
	}
	httpsPort, err := strconv.Atoi(GetEnv("CLICKHOUSE_HTTPS_PORT", "8443"))
	if err != nil {
		return ClickHouseTestEnvironment{}, nil
	}
	return ClickHouseTestEnvironment{
		Port:      port,
		HttpPort:  httpPort,
		SslPort:   sslPort,
		HttpsPort: httpsPort,
		Username:  GetEnv("CLICKHOUSE_USERNAME", "default"),
		Password:  GetEnv("CLICKHOUSE_PASSWORD", ""),
		Host:      GetEnv("CLICKHOUSE_HOST", "localhost"),
		Database:  GetEnv("CLICKHOUSE_DATABASE", getDatabaseName(testSet)),
	}, nil
}

func GetConnection(testSet string, settings clickhouse.Settings, tlsConfig *tls.Config, compression *clickhouse.Compression) (driver.Conn, error) {
	env, err := GetTestEnvironment(testSet)
	if err != nil {
		return nil, err
	}
	return getConnection(env, env.Database, settings, tlsConfig, compression)
}

func getConnection(env ClickHouseTestEnvironment, database string, settings clickhouse.Settings, tlsConfig *tls.Config, compression *clickhouse.Compression) (driver.Conn, error) {
	useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
	if err != nil {
		panic(err)
	}
	port := env.Port
	if useSSL && tlsConfig == nil {
		tlsConfig = &tls.Config{}
		port = env.SslPort
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Settings: settings,
		Auth: clickhouse.Auth{
			Database: database,
			Username: env.Username,
			Password: env.Password,
		},
		TLS:         tlsConfig,
		Compression: compression,
		DialTimeout: time.Duration(10) * time.Second,
	})
	return conn, err
}

func CreateDatabase(testSet string) error {
	env, err := GetTestEnvironment(testSet)
	if err != nil {
		return err
	}
	conn, err := getConnection(env, "default", nil, nil, nil)
	if err != nil {
		return err
	}
	return conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE `%s`", env.Database))
}

func getDatabaseName(testSet string) string {
	return fmt.Sprintf("clickhouse-go-%s-%s-%d", testSet, testUUID, testTimestamp)
}

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const numberBytes = "123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandAsciiString(n int) string {
	return randString(n, letterBytes)
}

func RandIntString(n int) string {
	return randString(n, numberBytes)
}

func RandIPv4() net.IP {
	return net.IPv4(uint8(rand.Int()), uint8(rand.Int()), uint8(rand.Int()), uint8(rand.Int())).To4()
}

func RandIPv6() net.IP {
	size := 16
	ip := make([]byte, size)
	for i := 0; i < size; i++ {
		ip[i] = byte(rand.Intn(256))
	}
	return net.IP(ip).To16()
}

func randString(n int, bytes string) string {
	sb := strings.Builder{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(bytes) {
			sb.WriteByte(bytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
// thanks to https://golangcode.com/print-the-current-memory-usage/
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
