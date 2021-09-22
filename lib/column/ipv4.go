package column

import (
	"net"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type IPv4 struct {
	base
}

func (*IPv4) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	v, err := decoder.Fixed(4)
	if err != nil {
		return nil, err
	}
	return net.IPv4(v[3], v[2], v[1], v[0]), nil
}

func (ip *IPv4) Write(encoder *binary.Encoder, v interface{}) error {
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

	r := netIP.To4()
	if len(r) != 4 {
		return &ErrUnexpectedType{
			T:      v,
			Column: ip,
		}
	}
	r[0], r[1], r[2], r[3] = r[3], r[2], r[1], r[0]
	if _, err := encoder.Write(r); err != nil {
		return err
	}
	return nil
}
