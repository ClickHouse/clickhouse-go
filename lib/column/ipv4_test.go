package column

import (
	"net"
	"testing"
)

const invalidIPv4Str = "44.489.38.222"

func TestIPv4_AppendRow_InvalidIP(t *testing.T) {
	col := IPv4{}

	// appending string
	err := col.AppendRow(invalidIPv4Str)
	if err == nil {
		t.Fatalf("AppendRow didn't IP")
	}
	switch e := err.(type) {
	case *ColumnConverterError:
		if e.Hint != "invalid IP format" {
			t.Fatalf("expected error hint %s, got %s", "AppendRow didn't IP", e.Hint)
		}
	default:
		t.Fatalf("AppendRow didn't IP")
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
		t.Fatal(err)
	}

	_, err = col.Append(strIps)
	if err == nil {
		t.Fatal("Append must return error if invalid IP provided")
	}
	if col.Rows() != 1 {
		t.Fatal("Append must preserve initial state if error happened")
	}
}

func TestIPv4_AppendRow(t *testing.T) {
	ip := net.ParseIP("43.58.38.33")
	strIp := ip.String()

	col := IPv4{}

	// appending string
	err := col.AppendRow(strIp)
	if err != nil {
		t.Fatal(err)
	}
	if col.Rows() != 1 {
		t.Fatalf("AppendRow didn't IP")
	}
	if !col.row(0).Equal(ip) {
		t.Fatalf("AppendRow resulted in %q instead of %q", col.row(0), ip)
	}

	// appending IP pointer
	err = col.AppendRow(&ip)
	if err != nil {
		t.Fatal(err)
	}
	if col.Rows() != 2 {
		t.Fatalf("AppendRow didn't IP")
	}
	if !col.row(1).Equal(ip) {
		t.Fatalf("AppendRow resulted in %q instead of %q", col.row(1), ip)
	}

	// appending IP
	err = col.AppendRow(ip)
	if err != nil {
		t.Fatal(err)
	}
	if col.Rows() != 3 {
		t.Fatalf("AppendRow didn't IP")
	}
	if !col.row(2).Equal(ip) {
		t.Fatalf("AppendRow resulted in %q instead of %q", col.row(3), ip)
	}

	// appending string pointer
	err = col.AppendRow(&strIp)
	if err != nil {
		t.Fatal(err)
	}
	if col.Rows() != 4 {
		t.Fatalf("AppendRow didn't IP")
	}
	if !col.row(3).Equal(ip) {
		t.Fatalf("AppendRow resulted in %q instead of %q", col.row(3), ip)
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
		t.Fatalf("Append added %d rows instead of %d", col.Rows(), len(strIps))
	}

	for i, ip := range ips {
		if !col.row(i).Equal(ip) {
			t.Fatalf("Append resulted in %q instead of %q", col.row(i), ip)
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
		t.Fatalf("Append added %d rows instead of %d", col.Rows(), len(strPtrIps))
	}

	for i, ip := range ips {
		if !col.row(i).Equal(ip) {
			t.Fatalf("ScanRow resulted in %q instead of %q", col.row(i), ip)
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
			t.Fatalf("unexpected ScanRow error: %v", err)
		}
		if !u.Equal(ips[i]) {
			t.Fatalf("ScanRow resulted in %q instead of %q", u, ips[i])
		}
	}

	// scanning strings
	for i := range ips {
		var u string
		err := col.ScanRow(&u, i)
		if err != nil {
			t.Fatalf("unexpected ScanRow error: %v", err)
		}
		if u != ips[i].String() {
			t.Fatalf("ScanRow resulted in %q instead of %q", u, ips[i])
		}
	}

	// scanning string pointers
	for i := range ips {
		var u *string
		err := col.ScanRow(&u, i)
		if err != nil {
			t.Fatalf("unexpected ScanRow error: %v", err)
		}
		if u == nil {
			t.Fatal("ScanRow resulted nil")
		}
		if *u != ips[i].String() {
			t.Fatalf("ScanRow resulted in %q instead of %q", *u, ips[i])
		}
	}
}
