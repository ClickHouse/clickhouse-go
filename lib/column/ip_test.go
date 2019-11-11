package column

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"io/ioutil"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_IPConverter(t *testing.T) {
	var ipAddresses = []string{
		"127.0.0.1",
		"99.67.1.100",
		"::1",
		"2001:0db8:0a0b:12f0:0000:0000:0000:0001",
		"2001:0db8::0001",
		"3731:54:65fe:2::a7",
	}
	for _, ips := range ipAddresses {
		var (
			ip2        IP
			ip         = net.ParseIP(ips)
			value, err = IP(ip).Value()
		)
		if assert.NoError(t, err) {
			if !strings.Contains(ips, ":") {
				vl := value.([]byte)
				err = ip2.Scan(vl[len(vl)-4:])
			} else {
				err = ip2.Scan(value)
			}
			if assert.NoError(t, err) {
				assert.True(t, ip.Equal(net.IP(ip2)), fmt.Sprintf("Invalid ip restore: %s != %s", ip, ip2))
			}
		}

		if assert.NoError(t, err) {
			err = ip2.Scan(ips)
			if assert.NoError(t, err) {
				assert.True(t, ip.Equal(net.IP(ip2)), fmt.Sprintf("Invalid ip restore: %s != %s", ip, ip2))
			}
		}
	}
	ip := IP(net.ParseIP(""))
	assert.Equal(t, errInvalidScanValue, ip.Scan(""))
	assert.Equal(t, errInvalidScanValue, ip.Scan([]byte{'1', '2', '3'}))
	assert.Equal(t, errInvalidScanType, ip.Scan(1))
}

func TestIPv4_Write(t *testing.T) {
	ip := net.ParseIP("0.0.0.0")
	ipv4 := IPv4{}

	cases := []struct {
		Key   interface{}
		Error interface{}
	}{
		{
			net.ParseIP("0.0.0.0"),
			nil,
		},
		{
			&ip,
			nil,
		},
		{
			"0.0.0.0",
			nil,
		},
		{
			"",
			&ErrUnexpectedType{
				&ipv4,
				"",
			},
		},
		{
			"2001:0db8::0001",
			&ErrUnexpectedType{
				&ipv4,
				"2001:0db8::0001",
			},
		},
	}
	for _, Case := range cases {

		buffer := binary.NewEncoder(ioutil.Discard)
		assert.Equal(t, ipv4.Write(buffer, Case.Key), Case.Error)
	}
}

func TestIPv6_Write(t *testing.T) {
	ip := net.ParseIP("2001:0db8::0001")
	ipv6 := IPv6{}

	cases := []struct {
		Key   interface{}
		Error interface{}
	}{
		{
			net.ParseIP("2001:0db8::0001"),
			nil,
		},
		{
			&ip,
			nil,
		},
		{
			"2001:0db8::0001",
			nil,
		},
		{
			"",
			&ErrUnexpectedType{
				&ipv6,
				"",
			},
		},
		{
			"0.0.0.0",
			nil,
		},
	}
	for _, Case := range cases {

		buffer := binary.NewEncoder(ioutil.Discard)
		assert.Equal(t, ipv6.Write(buffer, Case.Key), Case.Error)
	}
}