package column

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"reflect"

	"github.com/ClickHouse/ch-go/proto"
)

type IPv4 struct {
	name string
	col  proto.ColIPv4
}

func (col *IPv4) Reset() {
	col.col.Reset()
}

func (col *IPv4) Name() string {
	return col.name
}

func (col *IPv4) Type() Type {
	return "IPv4"
}

func (col *IPv4) ScanType() reflect.Type {
	return scanTypeIP
}

func (col *IPv4) Rows() int {
	return col.col.Rows()
}

func (col *IPv4) Row(i int, ptr bool) any {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *IPv4) ScanRow(dest any, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = col.row(row).String()
	case **string:
		*d = new(string)
		**d = col.row(row).String()
	case *net.IP:
		*d = col.row(row)
	case **net.IP:
		*d = new(net.IP)
		**d = col.row(row)
	case *netip.Addr:
		*d = col.rowAddr(row)
	case **netip.Addr:
		*d = new(netip.Addr)
		**d = col.rowAddr(row)
	case *uint32:
		ipV4 := col.row(row).To4()
		if ipV4 == nil {
			return &ColumnConverterError{
				Op:   "ScanRow",
				To:   fmt.Sprintf("%T", dest),
				From: "IPv4",
			}
		}
		*d = binary.BigEndian.Uint32(ipV4[:])
	case **uint32:
		ipV4 := col.row(row).To4()
		if ipV4 == nil {
			return &ColumnConverterError{
				Op:   "ScanRow",
				To:   fmt.Sprintf("%T", dest),
				From: "IPv4",
			}
		}
		*d = new(uint32)
		**d = binary.BigEndian.Uint32(ipV4[:])
	case sql.Scanner:
		return d.Scan(col.row(row))
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "IPv4",
		}
	}
	return nil
}

func strToIPV4(strIp string) (netip.Addr, error) {
	ip, err := netip.ParseAddr(strIp)
	if err != nil {
		return netip.Addr{}, &ColumnConverterError{
			Op:   "Append",
			To:   "IPv4",
			Hint: "invalid IP format",
		}
	}
	return ip, nil
}

func invalidIPv4Error(op, from string) error {
	return &ColumnConverterError{
		Op:   op,
		To:   "IPv4",
		From: from,
		Hint: "invalid IPv4 address",
	}
}

func toIPv4(ip netip.Addr, op string) (proto.IPv4, error) {
	ip = ip.Unmap()
	if !ip.Is4() {
		return 0, invalidIPv4Error(op, "netip.Addr")
	}
	return proto.ToIPv4(ip), nil
}

func (col *IPv4) appendV4IPs(ips []netip.Addr, nulls []uint8) error {
	values := make([]proto.IPv4, len(ips))
	for i := range ips {
		if len(nulls) != 0 && nulls[i] != 0 {
			continue
		}
		value, err := toIPv4(ips[i], "Append")
		if err != nil {
			return err
		}
		values[i] = value
	}
	for i := range values {
		col.col.Append(values[i])
	}
	return nil
}

func (col *IPv4) AppendV4IPs(ips []netip.Addr) error {
	return col.appendV4IPs(ips, nil)
}

func (col *IPv4) Append(v any) (nulls []uint8, err error) {

	switch v := v.(type) {
	case []string:
		nulls = make([]uint8, len(v))
		ips := make([]netip.Addr, len(v))
		for i := range v {
			ip, err := strToIPV4(v[i])
			if err != nil {
				return nulls, err
			}
			ips[i] = ip
		}
		if err := col.AppendV4IPs(ips); err != nil {
			return nulls, err
		}
	case []*string:
		nulls = make([]uint8, len(v))
		ips := make([]netip.Addr, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				ip, err := strToIPV4(*v[i])
				if err != nil {
					return nulls, err
				}
				ips[i] = ip
			default:
				ips[i] = netip.Addr{}
				nulls[i] = 1
			}
		}
		if err := col.appendV4IPs(ips, nulls); err != nil {
			return nulls, err
		}
	case []netip.Addr:
		nulls = make([]uint8, len(v))
		if err := col.AppendV4IPs(v); err != nil {
			return nulls, err
		}
	case []*netip.Addr:
		nulls = make([]uint8, len(v))
		ips := make([]netip.Addr, len(v))
		for i := range v {
			if v[i] == nil {
				nulls[i] = 1
				continue
			}
			ips[i] = *v[i]
		}
		if err := col.appendV4IPs(ips, nulls); err != nil {
			return nulls, err
		}
	case []net.IP:
		nulls = make([]uint8, len(v))
		values := make([]proto.IPv4, len(v))
		for i := range v {
			value, err := netIPToIPv4(v[i], "Append")
			if err != nil {
				return nulls, err
			}
			values[i] = value
		}
		for i := range values {
			col.col.Append(values[i])
		}
	case []*net.IP:
		nulls = make([]uint8, len(v))
		values := make([]proto.IPv4, len(v))
		for i := range v {
			if v[i] == nil {
				nulls[i] = 1
				continue
			}
			value, err := netIPToIPv4(*v[i], "Append")
			if err != nil {
				return nulls, err
			}
			values[i] = value
		}
		for i := range values {
			col.col.Append(values[i])
		}
	case []uint32:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(proto.IPv4(v[i]))
		}
	case []*uint32:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(proto.IPv4(*v[i]))
			default:
				nulls[i] = 1
				col.col.Append(0)
			}
		}
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "IPv4",
					From: fmt.Sprintf("%T", v),
					Hint: fmt.Sprintf("could not get driver.Valuer value, try using %s", col.Type()),
				}
			}
			return col.Append(val)
		}
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "IPv4",
			From: fmt.Sprintf("%T", v),
		}
	}

	return
}

func (col *IPv4) AppendRow(v any) (err error) {
	switch v := v.(type) {
	case string:
		ip, err := strToIPV4(v)
		if err != nil {
			return err
		}
		value, err := toIPv4(ip, "AppendRow")
		if err != nil {
			return err
		}
		col.col.Append(value)
	case *string:
		switch {
		case v != nil:
			ip, err := strToIPV4(*v)
			if err != nil {
				return err
			}
			value, err := toIPv4(ip, "AppendRow")
			if err != nil {
				return err
			}
			col.col.Append(value)
		default:
			col.col.Append(0)
		}
	case netip.Addr:
		value, err := toIPv4(v, "AppendRow")
		if err != nil {
			return err
		}
		col.col.Append(value)
	case *netip.Addr:
		switch {
		case v != nil:
			value, err := toIPv4(*v, "AppendRow")
			if err != nil {
				return err
			}
			col.col.Append(value)
		default:
			col.col.Append(0)
		}
	case net.IP:
		value, err := netIPToIPv4(v, "AppendRow")
		if err != nil {
			return err
		}
		col.col.Append(value)
	case *net.IP:
		switch {
		case v != nil:
			value, err := netIPToIPv4(*v, "AppendRow")
			if err != nil {
				return err
			}
			col.col.Append(value)
		default:
			col.col.Append(0)
		}
	case nil:
		col.col.Append(0)
	case uint32:
		col.col.Append(proto.IPv4(v))
	case *uint32:
		switch {
		case v != nil:
			col.col.Append(proto.IPv4(*v))
		default:
			col.col.Append(0)
		}
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "IPv4",
					From: fmt.Sprintf("%T", v),
					Hint: fmt.Sprintf("could not get driver.Valuer value, try using %s", col.Type()),
				}
			}
			return col.AppendRow(val)
		}
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "IPv4",
			From: fmt.Sprintf("%T", v),
		}
	}

	return
}

func (col *IPv4) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *IPv4) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

// TODO: This should probably return an netip.Addr
func (col *IPv4) row(i int) net.IP {
	src := col.col.Row(i).ToIP()
	ip := src.As4()
	return net.IPv4(ip[0], ip[1], ip[2], ip[3]).To4()
}

func (col *IPv4) rowAddr(i int) netip.Addr {
	return col.col.Row(i).ToIP()
}

func netIPToIPv4(ip net.IP, op string) (proto.IPv4, error) {
	if len(ip) == 0 {
		return 0, nil
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return 0, invalidIPv4Error(op, "net.IP")
	}
	return proto.IPv4(binary.BigEndian.Uint32(ip4)), nil
}

var _ Interface = (*IPv4)(nil)
