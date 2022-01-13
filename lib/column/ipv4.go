package column

import (
	"fmt"
	"net"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

const ipV4Size = 4

type IPv4 struct {
	data []byte
}

func (col *IPv4) Type() Type {
	return "IPv4"
}

func (col *IPv4) ScanType() reflect.Type {
	return scanTypeIP
}

func (col *IPv4) Rows() int {
	return len(col.data) / ipV4Size
}

func (col *IPv4) Row(i int) interface{} {
	return col.row(i)
}

func (col *IPv4) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *net.IP:
		*d = col.row(row)
	case **net.IP:
		*d = new(net.IP)
		**d = col.row(row)
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "IPv4",
		}
	}
	return nil
}

func (col *IPv4) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []net.IP:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			ip := v.To4()
			if ip == nil {
				return nil, &ColumnConverterErr{
					op:   "Append",
					to:   "IPv4",
					from: "IPv6",
				}
			}
			col.data = append(col.data, ip[:]...)
		}
	case []*net.IP:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				ip := v.To4()
				if ip == nil {
					return nil, &ColumnConverterErr{
						op:   "Append",
						to:   "IPv4",
						from: "IPv6",
					}
				}
				col.data = append(col.data, ip[:]...)
			default:
				col.data, nulls[i] = append(col.data, make([]byte, ipV4Size)...), 1
			}
		}
	default:
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   "IPv4",
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *IPv4) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case net.IP:
		ip := v.To4()
		if ip == nil {
			return &ColumnConverterErr{
				op:   "AppendRow",
				to:   "IPv4",
				from: "IPv6",
			}
		}
		col.data = append(col.data, ip[:]...)
	case null:
		col.data = append(col.data, make([]byte, ipV4Size)...)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "IPv4",
			from: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *IPv4) Decode(decoder *binary.Decoder, rows int) error {
	col.data = make([]byte, ipV4Size*rows)
	return decoder.Raw(col.data)
}

func (col *IPv4) Encode(encoder *binary.Encoder) error {
	return encoder.Raw(col.data)
}

func (col *IPv4) row(i int) net.IP {
	return col.data[i*ipV4Size : (i+1)*ipV4Size]
}

var _ Interface = (*IPv4)(nil)
