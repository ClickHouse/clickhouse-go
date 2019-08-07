package clickhouse

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/kshvakov/clickhouse/lib/binary"
)

type querySettingType int

// all possible query setting's data type
// TODO: support remaining data types
const (
	uintQS querySettingType = iota + 1
)

// description of single query setting
type querySettingInfo struct {
	name   string
	qsType querySettingType
}

// all possible query settings
// TODO: support remaining query serrings
var querySettingList = []querySettingInfo{
	{"max_memory_usage", uintQS},
	{"max_execution_time", uintQS},
	{"max_execution_speed", uintQS},
}

type querySettingValueEncoder func(enc *binary.Encoder) error

type querySettings struct {
	settings    map[string]querySettingValueEncoder
	settingsStr string // used for debug output
}

func makeQuerySettings(query url.Values) (*querySettings, error) {
	qs := &querySettings{
		settings:    make(map[string]querySettingValueEncoder),
		settingsStr: "",
	}

	for _, info := range querySettingList {
		valueStr := query.Get(info.name)
		if valueStr == "" {
			continue
		}

		switch info.qsType {
		case uintQS:
			value, err := strconv.ParseUint(valueStr, 10, 64)
			if err != nil {
				return nil, err
			}
			qs.settings[info.name] = func(enc *binary.Encoder) error { return enc.Uvarint(value) }
		default:
			err := fmt.Errorf("query setting %s has unsupported data type", info.name)
			return nil, err
		}

		if qs.settingsStr != "" {
			qs.settingsStr += "&"
		}
		qs.settingsStr += info.name + "=" + valueStr
	}

	return qs, nil
}

func (qs *querySettings) IsEmpty() bool {
	return len(qs.settings) == 0
}

func (qs *querySettings) Serialize(enc *binary.Encoder) error {
	for name, fn := range qs.settings {
		if err := enc.String(name); err != nil {
			return err
		}
		if err := fn(enc); err != nil {
			return err
		}
	}

	return nil
}
