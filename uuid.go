package clickhouse

import (
	"database/sql/driver"
	"encoding/hex"
	"errors"
)

var InvalidUUIDFormatError = errors.New("invalid UUID format")

type UUID string

func (str UUID) Value() (driver.Value, error) {
	return uuid2bytes(string(str))
}

func uuid2bytes(str string) ([]byte, error) {
	var uuid [16]byte
	if str[8] != '-' || str[13] != '-' || str[18] != '-' || str[23] != '-' {
		return nil, InvalidUUIDFormatError
	}
	for i, x := range [16]int{
		0, 2, 4, 6,
		9, 11, 14, 16,
		19, 21, 24, 26,
		28, 30, 32, 34,
	} {
		if v, ok := xtob(str[x], str[x+1]); !ok {
			return nil, InvalidUUIDFormatError
		} else {
			uuid[i] = v
		}
	}
	return uuid[:], nil
}

func bytes2uuid(src []byte) string {
	var uuid [36]byte
	hex.Encode(uuid[:], src[:4])
	uuid[8] = '-'
	hex.Encode(uuid[9:13], src[4:6])
	uuid[13] = '-'
	hex.Encode(uuid[14:18], src[6:8])
	uuid[18] = '-'
	hex.Encode(uuid[19:23], src[8:10])
	uuid[23] = '-'
	hex.Encode(uuid[24:], src[10:])
	return string(uuid[:])
}

// xvalues returns the value of a byte as a hexadecimal digit or 255.
var xvalues = [256]byte{
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 255, 255, 255, 255, 255, 255,
	255, 10, 11, 12, 13, 14, 15, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 10, 11, 12, 13, 14, 15, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
}

// xtob converts hex characters x1 and x2 into a byte.
func xtob(x1, x2 byte) (byte, bool) {
	b1 := xvalues[x1]
	b2 := xvalues[x2]
	return (b1 << 4) | b2, b1 != 255 && b2 != 255
}
