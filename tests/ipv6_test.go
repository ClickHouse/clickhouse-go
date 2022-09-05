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
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/stretchr/testify/require"
	"net"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIPv6(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_ipv6 (
				  Col1 IPv6
				, Col2 IPv6
				, Col3 Nullable(IPv6)
				, Col4 Array(IPv6)
				, Col5 Array(Nullable(IPv6))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_ipv6")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_ipv6")
	require.NoError(t, err)
	var (
		col1Data = net.ParseIP("2001:44c8:129:2632:33:0:252:2")
		col2Data = net.ParseIP("2a02:e980:1e::1")
		col3Data = col1Data
		col4Data = []net.IP{col1Data, col2Data}
		col5Data = []*net.IP{&col1Data, nil, &col2Data}
	)
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data))
	require.NoError(t, batch.Send())
	var (
		col1 net.IP
		col2 net.IP
		col3 *net.IP
		col4 []net.IP
		col5 []*net.IP
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_ipv6").Scan(&col1, &col2, &col3, &col4, &col5))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, col3Data, *col3)
	require.Len(t, col4, 2)
	assert.Equal(t, col1Data, col4[0])
	assert.Equal(t, col2Data, col4[1])
	require.Len(t, col5, 3)
	require.Nil(t, col5[1])
	assert.Equal(t, col1Data, *col5[0])
	assert.Equal(t, col2Data, *col5[2])
}

func TestIPv4InIPv6(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_ipv6 (
				  Col1 IPv6
				, Col2 IPv6
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_ipv6")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_ipv6")
	require.NoError(t, err)
	var (
		col1Data = net.ParseIP("127.0.0.1").To4()
		col2Data = net.ParseIP("85.242.48.167").To4()
	)
	require.NoError(t, batch.Append(col1Data, col2Data))
	require.NoError(t, batch.Send())
	var (
		col1 net.IP
		col2 net.IP
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_ipv6").Scan(&col1, &col2))
	assert.Equal(t, col1Data.To16(), col1)
	assert.Equal(t, col2Data.To16(), col2)
}

func TestNullableIPv6(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_ipv6 (
				  Col1 Nullable(IPv6)
				, Col2 Nullable(IPv6)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_ipv6")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_ipv6")
	require.NoError(t, err)
	var (
		col1Data = net.ParseIP("2a02:aa08:e000:3100::2")
		col2Data = net.ParseIP("2001:44c8:129:2632:33:0:252:2")
	)
	require.NoError(t, batch.Append(col1Data, col2Data))
	require.NoError(t, batch.Send())
	var (
		col1 *net.IP
		col2 *net.IP
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_ipv6").Scan(&col1, &col2))
	assert.Equal(t, col1Data, *col1)
	assert.Equal(t, col2Data, *col2)
	require.NoError(t, conn.Exec(ctx, "TRUNCATE TABLE test_ipv6"))
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_ipv6")
	require.NoError(t, err)
	col1Data = net.ParseIP("2001:44c8:129:2632:33:0:252:2")
	require.NoError(t, batch.Append(col1Data, nil))
	require.NoError(t, batch.Send())
	{
		var (
			col1 *net.IP
			col2 *net.IP
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_ipv6").Scan(&col1, &col2))
		require.Nil(t, col2)
		assert.Equal(t, col1Data, *col1)
	}
}

func TestColumnarIPv6(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_ipv6 (
				  Col1 IPv6
				, Col2 IPv6
				, Col3 Nullable(IPv6)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_ipv6")
	}()

	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_ipv6")
	require.NoError(t, err)
	var (
		col1Data []*net.IP
		col2Data []*net.IP
		col3Data []*net.IP
		v1, v2   = net.ParseIP("2001:44c8:129:2632:33:0:252:2"), net.ParseIP("192.168.1.1").To4()
	)
	col1Data = append(col1Data, &v1)
	col2Data = append(col2Data, &v2)
	col3Data = append(col3Data, nil)
	{
		batch.Column(0).Append(col1Data)
		batch.Column(1).Append(col2Data)
		batch.Column(2).Append(col3Data)
	}
	require.NoError(t, batch.Send())
	var (
		col1 *net.IP
		col2 *net.IP
		col3 *net.IP
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_ipv6").Scan(&col1, &col2, &col3))
	require.Nil(t, col3)
	require.Equal(t, v1, *col1)
	require.Equal(t, v2.To16(), *col2)
}

const invalidIPv6Str = "0:0:0:piyiy:0:0:0:1"

func getTestIPv6() []net.IP {
	return []net.IP{
		net.ParseIP("2001:db8:85a3:8d3:1319:8a2e:370:7348"),
		net.ParseIP("0:0:0:0:0:0:0:1"),
		net.ParseIP("::ffff:c000:0280"),
	}
}

func TestIPv6_AppendRow_InvalidIP(t *testing.T) {
	col := column.IPv6{}

	// appending string
	err := col.AppendRow(invalidIPv6Str)

	require.EqualError(t, err, (&column.ColumnConverterError{
		Op:   "Append",
		To:   "IPv6",
		Hint: "invalid IP format",
	}).Error())
}

func TestIPv6_Append_InvalidIP(t *testing.T) {
	strIps := []string{
		getTestIPv6()[0].String(),
		invalidIPv6Str,
	}

	// appending strings
	col := column.IPv6{}
	err := col.AppendRow(getTestIPv6()[1].String()) // add 1 valid IP

	require.NoError(t, err)

	_, err = col.Append(strIps)
	require.EqualError(t, err, (&column.ColumnConverterError{
		Op:   "Append",
		To:   "IPv6",
		Hint: "invalid IP format",
	}).Error())

	require.Equal(t, 1, col.Rows(), "Append must preserve initial state if error happened")
}

func TestIPv6_AppendRow(t *testing.T) {
	ip := getTestIPv6()[0]
	strIp := ip.String()

	col := column.IPv6{}

	// appending string
	err := col.AppendRow(strIp)

	require.NoError(t, err)
	require.Equal(t, 1, col.Rows(), "AppendRow didn't add IP")
	if !col.Row(0, false).(net.IP).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.Row(0, false), ip)
	}

	// appending IP pointer
	err = col.AppendRow(&ip)

	require.NoError(t, err)
	require.Equal(t, 2, col.Rows(), "AppendRow didn't add IP")
	if !col.Row(1, false).(net.IP).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.Row(1, false), ip)
	}

	// appending IP
	err = col.AppendRow(ip)

	require.NoError(t, err)
	require.Equal(t, 3, col.Rows(), "AppendRow didn't add IP")
	if !col.Row(2, false).(net.IP).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.Row(2, false), ip)
	}

	// appending string pointer
	err = col.AppendRow(&strIp)

	require.NoError(t, err)
	require.Equal(t, 4, col.Rows(), "AppendRow didn't add IP")
	if !col.Row(3, false).(net.IP).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.Row(3, false), ip)
	}
}

func TestIPv6_Append(t *testing.T) {
	ips := getTestIPv6()

	var strIps []string

	for _, ip := range ips {
		strIps = append(strIps, ip.String())
	}

	// appending strings
	col := column.IPv6{}
	_, err := col.Append(strIps)

	require.NoError(t, err)
	require.Equalf(t, col.Rows(), len(strIps), "AppendRow didn't add IP", "Added %d rows instead of %d", col.Rows(), len(strIps))
	for i, ip := range ips {
		if !col.Row(i, false).(net.IP).Equal(ip) {
			require.Failf(t, "Invalid result of Append", "Added %q instead of %q", col.Row(i, false), ip)
		}
	}

	// appending string pointers
	var strPtrIps []*string

	for _, ip := range ips {
		str := ip.String()
		strPtrIps = append(strPtrIps, &str)
	}

	col = column.IPv6{}
	_, err = col.Append(strPtrIps)

	require.NoError(t, err)
	require.Equalf(t, col.Rows(), len(strPtrIps), "Added %d rows instead of %d", col.Rows(), len(strPtrIps))
	for i, ip := range ips {
		if !col.Row(i, false).(net.IP).Equal(ip) {
			require.Failf(t, "Invalid result of Append", "Added %q instead of %q", col.Row(i, false), ip)
		}
	}
}

func TestIPv6_ScanRow(t *testing.T) {
	ips := getTestIPv6()

	col := column.IPv6{}
	_, err := col.Append(ips)
	if err != nil {
		t.Fatal(err)
	}

	// scanning ips
	for i := range ips {
		var u net.IP
		err := col.ScanRow(&u, i)
		require.NoError(t, err)
		if !u.Equal(ips[i]) {
			require.Failf(t, "Invalid result of ScanRow", "ScanRow resulted in %q instead of %q", u, ips[i])
		}
	}

	// scanning strings
	for i := range ips {
		var u string
		err := col.ScanRow(&u, i)
		require.NoError(t, err)
		require.Equal(t, ips[i].String(), u)
	}

	// scanning string pointers
	for i := range ips {
		var u *string
		err := col.ScanRow(&u, i)
		require.NoError(t, err)
		require.NotNilf(t, u, "ScanRow resulted nil")
		require.Equal(t, *u, ips[i].String(), "ScanRow resulted in %q instead of %q", *u, ips[i])
	}
}

func TestIPv6Flush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_ipv6_ring_flush (
			  Col1 IPv6
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_ipv6_ring_flush")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_ipv6_ring_flush")
	require.NoError(t, err)
	vals := [1000]net.IP{}
	for i := 0; i < 1000; i++ {
		vals[i] = RandIPv6()
		require.NoError(t, batch.Append(vals[i]))
		require.NoError(t, batch.Flush())
	}
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_ipv6_ring_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 net.IP
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}
