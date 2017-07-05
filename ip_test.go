package clickhouse

import (
	"net"
	"strings"
	"testing"
)

var ipAddresses = []string{
	"127.0.0.1",
	"99.67.1.100",
	"::1",
	"2001:0db8:0a0b:12f0:0000:0000:0000:0001",
	"2001:0db8::0001",
	"3731:54:65fe:2::a7",
}

func Test_IPConverter(t *testing.T) {
	for _, ips := range ipAddresses {
		var (
			ip2        IP
			ip         = net.ParseIP(ips)
			value, err = IP(ip).Value()
		)

		if err != nil {
			t.Error(err)
			return
		}

		if err == nil {
			if !strings.Contains(ips, ":") {
				vl := value.([]byte)
				err = ip2.Scan(vl[len(vl)-4:])
			} else {
				err = ip2.Scan(value)
			}
		}

		if !ip.Equal(net.IP(ip2)) {
			t.Errorf("Invalid ip restore: %s != %s", ip, ip2)
		}
	}
}
