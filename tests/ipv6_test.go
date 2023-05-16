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
	"github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/stretchr/testify/require"
	"net"
	"net/netip"
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
			    , Col6 Array(IPv6)
				, Col7 Array(Nullable(IPv6))
			    , Col8 IPv6
				, Col9 Nullable(IPv6)
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
		col6Data = []*net.IP{&col1Data, &col2Data}
		col7Data = []*net.IP{&col1Data, nil, &col2Data}
		col8Data = &col1Data
		col9Data = col2Data
	)
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data, col8Data, col9Data))
	require.NoError(t, batch.Send())
	var (
		col1 net.IP
		col2 net.IP
		col3 *net.IP
		col4 []net.IP
		col5 []*net.IP
		col6 []net.IP
		col7 []*net.IP
		col8 [16]byte
		col9 *[16]byte
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_ipv6").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9))
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

	require.Len(t, col6, 2)
	assert.Equal(t, col1Data, col4[0])
	assert.Equal(t, col2Data, col4[1])
	require.Len(t, col7, 3)
	require.Nil(t, col7[1])
	assert.Equal(t, col1Data, *col7[0])
	assert.Equal(t, col2Data, *col7[2])

	assert.Equal(t, col1Data, net.ParseIP(netip.AddrFrom16(col8).String()))
	assert.Equal(t, col2Data, net.ParseIP(netip.AddrFrom16(*col9).String()))

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
				, Col3 IPv6
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
		col3Data = net.ParseIP("85.242.48.167").To4()
	)
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data))
	require.NoError(t, batch.Send())
	var (
		col1 net.IP
		col2 net.IP
		col3 [16]byte
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_ipv6").Scan(&col1, &col2, &col3))
	assert.Equal(t, col1Data.To16(), col1)
	assert.Equal(t, col2Data.To16(), col2)
	assert.Equal(t, col3Data.To16(), net.ParseIP(netip.AddrFrom16(col3).String()))
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
		col1Data = net.ParseIP("2a02:aa08:e000:3100::2")
		col2Data = net.ParseIP("2001:44c8:129:2632:33:0:252:2")
		col3Data = net.ParseIP("2001:44c8:129:2632:33:0:252:2")
	)
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data))
	require.NoError(t, batch.Send())
	var (
		col1 *net.IP
		col2 *net.IP
		col3 *[16]byte
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_ipv6").Scan(&col1, &col2, &col3))
	assert.Equal(t, col1Data, *col1)
	assert.Equal(t, col2Data, *col2)
	assert.Equal(t, col3Data, net.ParseIP(netip.AddrFrom16(*col3).String()))
	require.NoError(t, conn.Exec(ctx, "TRUNCATE TABLE test_ipv6"))
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_ipv6")
	require.NoError(t, err)
	col1Data = net.ParseIP("2001:44c8:129:2632:33:0:252:2")
	require.NoError(t, batch.Append(col1Data, nil, nil))
	require.NoError(t, batch.Send())
	{
		var (
			col1 *net.IP
			col2 *net.IP
			col3 *[16]byte
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_ipv6").Scan(&col1, &col2, &col3))
		require.Nil(t, col2)
		require.Nil(t, col3)
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
				, Col4 IPv6
				, Col5 Nullable(IPv6)
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
		col4Data []*[16]byte
		col5Data []*[16]byte
		v1, v2   = net.ParseIP("2001:44c8:129:2632:33:0:252:2"), net.ParseIP("192.168.1.1").To4()
	)
	col1Data = append(col1Data, &v1)
	col2Data = append(col2Data, &v2)
	col3Data = append(col3Data, nil)
	col4Data = append(col4Data, &[][16]byte{netip.MustParseAddr(v1.String()).As16()}[0])
	col5Data = append(col5Data, nil)
	{
		batch.Column(0).Append(col1Data)
		batch.Column(1).Append(col2Data)
		batch.Column(2).Append(col3Data)
		batch.Column(3).Append(col4Data)
		batch.Column(4).Append(col5Data)
	}
	require.NoError(t, batch.Send())
	var (
		col1 *net.IP
		col2 *net.IP
		col3 *net.IP
		col4 *[16]byte
		col5 *[16]byte
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_ipv6").Scan(&col1, &col2, &col3, &col4, &col5))
	require.Nil(t, col3)
	require.Nil(t, col5)
	require.Equal(t, v1, *col1)
	require.Equal(t, v2.To16(), *col2)
	require.Equal(t, v1.To16(), net.ParseIP(netip.AddrFrom16(*col4).String()).To16())
}

const invalidIPv6Str = "0:0:0:piyiy:0:0:0:1"

func getTestIPv6() []net.IP {
	return []net.IP{
		net.ParseIP("2001:db8:85a3:8d3:1319:8a2e:370:7348"),
		net.ParseIP("0:0:0:0:0:0:0:1"),
		net.ParseIP("::ffff:c000:0280"),
	}
}

func TestIPv6AppendRowInvalidIP(t *testing.T) {
	col := column.IPv6{}

	// appending string
	err := col.AppendRow(invalidIPv6Str)

	require.EqualError(t, err, (&column.ColumnConverterError{
		Op:   "Append",
		To:   "IPv6",
		Hint: "invalid IP format",
	}).Error())
}

func TestIPv6Append_InvalidIP(t *testing.T) {
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

func assertRowColumnEqualToIP(t *testing.T, value any, ip net.IP) {
	require.IsType(t, net.IP{}, value, "Invalid type of column value. net.IP expected.")
	assert.Truef(t, value.(net.IP).Equal(ip), "%q is not equal to IP %q", value, ip)
}

func TestIPv6AppendRow(t *testing.T) {
	ip := getTestIPv6()[0]
	strIp := ip.String()
	ipBytes := netip.MustParseAddr(strIp).As16()
	col := column.IPv6{}

	rows := []any{
		strIp,                     // appending string
		&ip,                       // appending IP pointer
		ip,                        // appending IP
		&strIp,                    // appending string pointer
		ipBytes,                   // appending [16]byte
		proto.IPv6(ipBytes),       // appending proto.IPv6
		&[][16]byte{ipBytes}[0],   // appending [16]byte pointer
		&[]proto.IPv6{ipBytes}[0], // appending proto.IPv6 pointer
	}
	for i, row := range rows {
		err := col.AppendRow(row)
		assert.Nil(t, err)
		assert.Truef(t, i+1 == col.Rows(), "AppendRow didn't add IP")
		assertRowColumnEqualToIP(t, col.Row(i, false), ip)
	}
}

func testAppend[T any](t *testing.T, ips []net.IP, convertor func(ip net.IP) T) {
	var strIps []T

	for _, ip := range ips {
		strIps = append(strIps, convertor(ip))
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
}

func TestIPv6Append(t *testing.T) {
	ips := getTestIPv6()

	testAppend(t, ips, func(ip net.IP) string {
		return ip.String()
	})
	testAppend(t, ips, func(ip net.IP) *string {
		str := ip.String()
		return &str
	})
	testAppend(t, ips, func(ip net.IP) [16]byte {
		return netip.MustParseAddr(ip.String()).As16()
	})
	testAppend(t, ips, func(ip net.IP) *[16]byte {
		return &[][16]byte{netip.MustParseAddr(ip.String()).As16()}[0]
	})
	testAppend(t, ips, func(ip net.IP) proto.IPv6 {
		return netip.MustParseAddr(ip.String()).As16()
	})
	testAppend(t, ips, func(ip net.IP) *proto.IPv6 {
		return &[]proto.IPv6{netip.MustParseAddr(ip.String()).As16()}[0]
	})

}

func testScanRow[T any](t *testing.T, col *column.IPv6, ips []net.IP, convertor func(result T, src net.IP) bool) {
	for i := range ips {
		var u T
		err := col.ScanRow(&u, i)
		require.NoError(t, err)
		if !convertor(u, ips[i]) {
			require.Failf(t, "Invalid result of ScanRow", "ScanRow resulted in %q instead of %q", u, ips[i])
		}
	}

}

func TestIPv6ScanRow(t *testing.T) {
	ips := getTestIPv6()

	col := column.IPv6{}
	_, err := col.Append(ips)
	if err != nil {
		t.Fatal(err)
	}

	// scanning ips
	testScanRow(t, &col, ips, func(result net.IP, src net.IP) bool {
		return result.Equal(src)
	})

	// scanning strings
	testScanRow(t, &col, ips, func(result string, src net.IP) bool {
		return src.String() == result
	})

	// scanning string pointers
	testScanRow(t, &col, ips, func(result *string, src net.IP) bool {
		return src.String() == *result
	})

	// scanning [16]byte
	testScanRow(t, &col, ips, func(result [16]byte, src net.IP) bool {
		expected := netip.MustParseAddr(src.String()).As16()
		for i := range expected {
			if expected[i] != result[i] {
				return false
			}
		}
		return true
	})

	// scanning [16]byte pointer
	testScanRow(t, &col, ips, func(result *[16]byte, src net.IP) bool {
		expected := netip.MustParseAddr(src.String()).As16()
		for i := range expected {
			if expected[i] != (*result)[i] {
				return false
			}
		}
		return true
	})
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
