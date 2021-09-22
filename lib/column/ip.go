/*
IP type supporting for clickhouse as FixedString(16)
*/

package column

import (
	"database/sql/driver"
	"errors"
	"net"
	"strings"
)

var (
	errInvalidScanType  = errors.New("invalid scan types")
	errInvalidScanValue = errors.New("invalid scan value")
)

// IP column type
type IP net.IP

// Value implements the driver.Valuer interface, json field interface
// Alignment on the right side
func (ip IP) Value() (driver.Value, error) {
	return ip.MarshalBinary()
}

func (ip IP) MarshalBinary() ([]byte, error) {
	if len(ip) < 16 {
		var (
			buff = make([]byte, 16)
			j    = 0
		)
		for i := 16 - len(ip); i < 16; i++ {
			buff[i] = ip[j]
			j++
		}
		for i := 0; i < 16-len(ip); i++ {
			buff[i] = '\x00'
		}
		if len(ip) == 4 {
			buff[11] = '\xff'
			buff[10] = '\xff'
		}
		return buff, nil
	}
	return []byte(ip), nil
}

// Scan implements the driver.Valuer interface, json field interface
func (ip *IP) Scan(value interface{}) (err error) {
	switch v := value.(type) {
	case []byte:
		if len(v) == 4 || len(v) == 16 {
			*ip = IP(v)
		} else {
			err = errInvalidScanValue
		}
	case string:
		if v == "" {
			err = errInvalidScanValue
			return
		}
		if (len(v) == 4 || len(v) == 16) && !strings.Contains(v, ".") && !strings.Contains(v, ":") {
			*ip = IP([]byte(v))
			return
		}
		if strings.Contains(v, ":") {
			*ip = IP(net.ParseIP(v))
			return
		}
		*ip = IP(net.ParseIP(v).To4())
	case net.IP:
		*ip = IP(v)
	default:
		err = errInvalidScanType
	}
	return
}

// String implements the fmt.Stringer interface
func (ip IP) String() string {
	return net.IP(ip).String()
}
