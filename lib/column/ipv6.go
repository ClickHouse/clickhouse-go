package column

import (
	"net"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type IPv6 struct {
	base
}

func (*IPv6) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	v, err := decoder.Fixed(16)
	if err != nil {
		return nil, err
	}
	return net.IP(v), nil
}

func (ip *IPv6) Write(encoder *binary.Encoder, v interface{}) error {
	var netIP net.IP
	switch x := v.(type) {
	case string:
		netIP = net.ParseIP(x)
	case net.IP:
		netIP = x
	case *net.IP:
		if x != nil {
			netIP = *x
		}
	}

	r := netIP.To16()
	if len(r) != 16 {
		return &ErrUnexpectedType{
			T:      v,
			Column: ip,
		}
	}
	if _, err := encoder.Write(r); err != nil {
		return err
	}
	return nil
}
