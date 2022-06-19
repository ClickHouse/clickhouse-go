package column

import (
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

const invalidIPv4Str = "44.489.38.222"

func TestIPv4_AppendRow_InvalidIP(t *testing.T) {
	col := IPv4{}

	// appending string
	err := col.AppendRow(invalidIPv4Str)
	if err == nil {
		require.Fail(t, "AppendRow must return error if invalid IP provided")
	}

	switch e := err.(type) {
	case *ColumnConverterError:
		require.Equal(t, "invalid IP format", e.Hint)
	default:
		require.Error(t, err, "AppendRow returned unexpected error")
	}
}

func TestIPv4_Append_InvalidIP(t *testing.T) {
	strIps := []string{
		"43.58.38.33",
		invalidIPv4Str,
	}

	// appending strings
	col := IPv4{}
	err := col.AppendRow("127.0.0.1") // add 1 valid IP
	if err != nil {
		require.Error(t, err)
	}

	_, err = col.Append(strIps)
	if err == nil {
		require.Fail(t, "Append must return error if invalid IP provided")

	}
	if col.Rows() != 1 {
		require.Fail(t, "Append must preserve initial state if error happened")
	}
}

func TestIPv4_AppendRow(t *testing.T) {
	ip := net.ParseIP("43.58.38.33")
	strIp := ip.String()

	col := IPv4{}

	// appending string
	err := col.AppendRow(strIp)
	if err != nil {
		require.Error(t, err)
	}
	if col.Rows() != 1 {
		require.Fail(t, "AppendRow didn't add IP")
	}

	if !col.row(0).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.row(0), ip)
	}

	// appending IP pointer
	err = col.AppendRow(&ip)
	if err != nil {
		t.Fatal(err)
	}
	if col.Rows() != 2 {
		require.Fail(t, "AppendRow didn't add IP")
	}
	if !col.row(1).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.row(1), ip)
	}

	// appending IP
	err = col.AppendRow(ip)
	if err != nil {
		require.Error(t, err)
	}
	if col.Rows() != 3 {
		require.Fail(t, "AppendRow didn't add IP")
	}
	if !col.row(2).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.row(2), ip)
	}

	// appending string pointer
	err = col.AppendRow(&strIp)
	if err != nil {
		t.Fatal(err)
	}
	if col.Rows() != 4 {
		require.Fail(t, "AppendRow didn't add IP")
	}
	if !col.row(3).Equal(ip) {
		require.Failf(t, "Invalid result of AppendRow", "Added %q instead of %q", col.row(3), ip)
	}
}

func TestIPv4_Append(t *testing.T) {
	ips := []net.IP{
		net.ParseIP("43.58.38.33"),
		net.ParseIP("127.0.0.1"),
		net.ParseIP("192.168.18.38"),
	}

	var strIps []string

	for _, ip := range ips {
		strIps = append(strIps, ip.String())
	}

	// appending strings
	col := IPv4{}
	_, err := col.Append(strIps)
	if err != nil {
		t.Fatal(err)
	}

	if col.Rows() != len(strIps) {
		require.Failf(t, "Invalid result of Append", "Added %d rows instead of %d", col.Rows(), len(strIps))
	}

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

	col = IPv4{}
	_, err = col.Append(strPtrIps)
	if err != nil {
		t.Fatal(err)
	}

	if col.Rows() != len(strPtrIps) {
		require.Failf(t, "Invalid result of Append", "Added %d rows instead of %d", col.Rows(), len(strPtrIps))
	}

	for i, ip := range ips {
		if !col.row(i).Equal(ip) {
			require.Failf(t, "Invalid result of Append", "Added %q instead of %q", col.row(i), ip)
		}
	}
}

func TestIp4_ScanRow(t *testing.T) {
	ips := []net.IP{
		net.ParseIP("43.58.38.33"),
		net.ParseIP("127.0.0.1"),
		net.ParseIP("192.168.18.38"),
	}

	col := IPv4{}
	_, err := col.Append(ips)
	if err != nil {
		t.Fatal(err)
	}

	// scanning ips
	for i := range ips {
		var u net.IP
		err := col.ScanRow(&u, i)
		if err != nil {
			require.Error(t, err, "unexpected ScanRow error")
		}
		if !u.Equal(ips[i]) {
			require.Failf(t, "Invalid result of ScanRow", "ScanRow resulted in %q instead of %q", u, ips[i])
		}
	}

	// scanning strings
	for i := range ips {
		var u string
		err := col.ScanRow(&u, i)
		if err != nil {
			require.Error(t, err, "unexpected ScanRow error")
		}
		if u != ips[i].String() {
			require.Failf(t, "Invalid result of ScanRow", "ScanRow resulted in %q instead of %q", u, ips[i])
		}
	}

	// scanning string pointers
	for i := range ips {
		var u *string
		err := col.ScanRow(&u, i)
		if err != nil {
			require.Error(t, err, "unexpected ScanRow error")
		}
		if u == nil {
			require.Fail(t, "ScanRow resulted nil")
		}
		if *u != ips[i].String() {
			require.Failf(t, "Invalid result of ScanRow", "ScanRow resulted in %q instead of %q", *u, ips[i])
		}
	}
}
