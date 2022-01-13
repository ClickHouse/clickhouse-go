package column

import (
	"fmt"
	"net"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

const ipV6Size = 16

type IPv6 struct {
	data []byte
}

func (col *IPv6) Type() Type {
	return "IPv6"
}

func (col *IPv6) ScanType() reflect.Type {
	return scanTypeIP
}

func (col *IPv6) Rows() int {
	return len(col.data) / ipV6Size
}

func (col *IPv6) Row(i int) interface{} {
	return col.row(i)
}

func (col *IPv6) ScanRow(dest interface{}, row int) error {
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
			from: "IPv6",
		}
	}
	return nil
}

func (col *IPv6) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []net.IP:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			if ip := v.To4(); ip != nil {
				return nil, &ColumnConverterErr{
					op:   "Append",
					to:   "IPv6",
					from: "IPv4",
				}
			}
			col.data = append(col.data, v[:]...)
		}
	case []*net.IP:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				if ip := v.To4(); ip != nil {
					return nil, &ColumnConverterErr{
						op:   "Append",
						to:   "IPv6",
						from: "IPv4",
					}
				}
				tmp := *v
				col.data = append(col.data, tmp[:]...)
			default:
				col.data, nulls[i] = append(col.data, make([]byte, ipV6Size)...), 1
			}
		}
	default:
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   "IPv6",
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *IPv6) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case net.IP:
		if ip := v.To4(); ip != nil {
			return &ColumnConverterErr{
				op:   "AppendRow",
				to:   "IPv6",
				from: "IPv4",
			}
		}
		col.data = append(col.data, v[:]...)
	case null:
		col.data = append(col.data, make([]byte, ipV6Size)...)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "IPv6",
			from: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *IPv6) Decode(decoder *binary.Decoder, rows int) error {
	col.data = make([]byte, ipV6Size*rows)
	return decoder.Raw(col.data)
}

func (col *IPv6) Encode(encoder *binary.Encoder) error {
	return encoder.Raw(col.data)
}

func (col *IPv6) row(i int) net.IP {
	return col.data[i*ipV6Size : (i+1)*ipV6Size]
}

var _ Interface = (*IPv6)(nil)
