package column

import (
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIPv4AppendRejectsNonIPv4(t *testing.T) {
	ipv6String := "2001:db8::1"
	ipv6Addr := netip.MustParseAddr(ipv6String)
	ipv6NetIP := net.ParseIP(ipv6String)

	tests := []struct {
		name   string
		append func(*IPv4) error
	}{
		{
			name: "string row",
			append: func(col *IPv4) error {
				return col.AppendRow(ipv6String)
			},
		},
		{
			name: "string pointer row",
			append: func(col *IPv4) error {
				return col.AppendRow(&ipv6String)
			},
		},
		{
			name: "netip row",
			append: func(col *IPv4) error {
				return col.AppendRow(ipv6Addr)
			},
		},
		{
			name: "netip pointer row",
			append: func(col *IPv4) error {
				return col.AppendRow(&ipv6Addr)
			},
		},
		{
			name: "net.IP row",
			append: func(col *IPv4) error {
				return col.AppendRow(ipv6NetIP)
			},
		},
		{
			name: "net.IP pointer row",
			append: func(col *IPv4) error {
				return col.AppendRow(&ipv6NetIP)
			},
		},
		{
			name: "string bulk",
			append: func(col *IPv4) error {
				_, err := col.Append([]string{ipv6String})
				return err
			},
		},
		{
			name: "string pointer bulk",
			append: func(col *IPv4) error {
				_, err := col.Append([]*string{&ipv6String})
				return err
			},
		},
		{
			name: "netip bulk",
			append: func(col *IPv4) error {
				_, err := col.Append([]netip.Addr{ipv6Addr})
				return err
			},
		},
		{
			name: "netip pointer bulk",
			append: func(col *IPv4) error {
				_, err := col.Append([]*netip.Addr{&ipv6Addr})
				return err
			},
		},
		{
			name: "net.IP bulk",
			append: func(col *IPv4) error {
				_, err := col.Append([]net.IP{ipv6NetIP})
				return err
			},
		},
		{
			name: "net.IP pointer bulk",
			append: func(col *IPv4) error {
				_, err := col.Append([]*net.IP{&ipv6NetIP})
				return err
			},
		},
		{
			name: "netip helper",
			append: func(col *IPv4) error {
				return col.AppendV4IPs([]netip.Addr{ipv6Addr})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &IPv4{}
			var err error
			require.NotPanics(t, func() {
				err = tt.append(col)
			})
			require.Error(t, err)
			require.IsType(t, &ColumnConverterError{}, err)
			require.Equal(t, 0, col.Rows())
		})
	}
}

func TestIPv4AppendAcceptsIPv4MappedIPv6(t *testing.T) {
	mappedAddr := netip.MustParseAddr("::ffff:192.0.2.1")
	mappedNetIP := net.ParseIP("::ffff:192.0.2.1")

	tests := []struct {
		name   string
		append func(*IPv4) error
	}{
		{
			name: "netip row",
			append: func(col *IPv4) error {
				return col.AppendRow(mappedAddr)
			},
		},
		{
			name: "net.IP row",
			append: func(col *IPv4) error {
				return col.AppendRow(mappedNetIP)
			},
		},
		{
			name: "netip bulk",
			append: func(col *IPv4) error {
				_, err := col.Append([]netip.Addr{mappedAddr})
				return err
			},
		},
		{
			name: "net.IP bulk",
			append: func(col *IPv4) error {
				_, err := col.Append([]net.IP{mappedNetIP})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &IPv4{}
			require.NoError(t, tt.append(col))
			require.Equal(t, "192.0.2.1", col.Row(0, false).(net.IP).String())
		})
	}
}
