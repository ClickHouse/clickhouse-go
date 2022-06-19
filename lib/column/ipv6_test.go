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

package column

import (
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

const invalidIPv6Str = "0:0:0:piyiy:0:0:0:1"

func getTestIPv6() []net.IP {
	return []net.IP{
		net.ParseIP("2001:db8:85a3:8d3:1319:8a2e:370:7348"),
		net.ParseIP("0:0:0:0:0:0:0:1"),
		net.ParseIP("::ffff:c000:0280"),
	}
}

func TestIPv6_AppendRow_InvalidIP(t *testing.T) {
	col := IPv6{}

	// appending string
	err := col.AppendRow(invalidIPv6Str)

	require.EqualError(t, err, (&ColumnConverterError{
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
	col := IPv6{}
	err := col.AppendRow(getTestIPv6()[1].String()) // add 1 valid IP

	require.NoError(t, err)

	_, err = col.Append(strIps)
	require.EqualError(t, err, (&ColumnConverterError{
		Op:   "Append",
		To:   "IPv6",
		Hint: "invalid IP format",
	}).Error())

	require.Equal(t, 1, col.Rows(), "Append must preserve initial state if error happened")
}

func TestIPv6_AppendRow(t *testing.T) {
	ip := getTestIPv6()[0]
	strIp := ip.String()

	col := IPv6{}

	// appending string
	err := col.AppendRow(strIp)

	require.NoError(t, err)
	require.Equal(t, 1, col.Rows(), "AppendRow didn't add IP")
	if !col.row(0).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.row(0), ip)
	}

	// appending IP pointer
	err = col.AppendRow(&ip)

	require.NoError(t, err)
	require.Equal(t, 2, col.Rows(), "AppendRow didn't add IP")
	if !col.row(1).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.row(1), ip)
	}

	// appending IP
	err = col.AppendRow(ip)

	require.NoError(t, err)
	require.Equal(t, 3, col.Rows(), "AppendRow didn't add IP")
	if !col.row(2).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.row(2), ip)
	}

	// appending string pointer
	err = col.AppendRow(&strIp)

	require.NoError(t, err)
	require.Equal(t, 4, col.Rows(), "AppendRow didn't add IP")
	if !col.row(3).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.row(3), ip)
	}
}

func TestIPv6_Append(t *testing.T) {
	ips := getTestIPv6()

	var strIps []string

	for _, ip := range ips {
		strIps = append(strIps, ip.String())
	}

	// appending strings
	col := IPv6{}
	_, err := col.Append(strIps)

	require.NoError(t, err)
	require.Equalf(t, col.Rows(), len(strIps), "AppendRow didn't add IP", "Added %d rows instead of %d", col.Rows(), len(strIps))
	for i, ip := range ips {
		if !col.row(i).Equal(ip) {
			require.Failf(t, "Invalid result of Append", "Added %q instead of %q", col.row(i), ip)
		}
	}

	// appending string pointers
	var strPtrIps []*string

	for _, ip := range ips {
		str := ip.String()
		strPtrIps = append(strPtrIps, &str)
	}

	col = IPv6{}
	_, err = col.Append(strPtrIps)

	require.NoError(t, err)
	require.Equalf(t, col.Rows(), len(strPtrIps), "Added %d rows instead of %d", col.Rows(), len(strPtrIps))
	for i, ip := range ips {
		if !col.row(i).Equal(ip) {
			require.Failf(t, "Invalid result of Append", "Added %q instead of %q", col.row(i), ip)
		}
	}
}

func TestIPv6_ScanRow(t *testing.T) {
	ips := getTestIPv6()

	col := IPv6{}
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
