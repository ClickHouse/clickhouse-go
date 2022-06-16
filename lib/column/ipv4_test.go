package column

import (
	"net"
	"testing"
)

func getTestIps4() []net.IP {
	return []net.IP{
		net.ParseIP("43.58.38.33"),
		net.ParseIP("127.0.0.1"),
		net.ParseIP("192.168.18.38"),
	}
}

func TestIp4_ScanRow(t *testing.T) {
	ips := getTestIps4()

	col := IPv4{}
	_, err := col.Append(ips)
	if err != nil {
		t.Fatal(err)
	}

	// scanning uuid.UUID
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
}
