// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package timezone

import (
	"errors"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var cache = struct {
	mutex sync.Mutex
	items map[string]*time.Location
}{
	items: make(map[string]*time.Location),
}

var (
	timeRegex1, _ = regexp.Compile(timeRegexPattern1)
	timeRegex2, _ = regexp.Compile(timeRegexPattern2)
	timeRegex3, _ = regexp.Compile(timeRegexPattern3)
)

const (
	// Regular expression1(datetime separator supports '-', '/', '.').
	// Eg:
	// "2017-12-14 04:51:34 +0805 LMT",
	// "2017-12-14 04:51:34 +0805 LMT",
	// "2006-01-02T15:04:05Z07:00",
	// "2014-01-17T01:19:15+08:00",
	// "2018-02-09T20:46:17.897Z",
	// "2018-02-09 20:46:17.897",
	// "2018-02-09T20:46:17Z",
	// "2018-02-09 20:46:17",
	// "2018/10/31 - 16:38:46"
	// "2018-02-09",
	// "2018.02.09",
	timeRegexPattern1 = `(\d{4}[./-]\d{1,2}[./-]\d{1,2})[T\s-]*(\d{1,2}:\d{1,2}:\d{1,2})?(\.\d{3})?[\s/+Zz]*(\d{4})?(\d{1,2}:\d{1,2})?`

	// Regular expression2(datetime separator supports '-', '/', '.').
	// Eg:
	// 01-Nov-2018 11:50:28
	// 01/Nov/2018 11:50:28
	// 01.Nov.2018 11:50:28
	// 01.Nov.2018:11:50:28
	timeRegexPattern2 = `(\d{1,2}[./-][A-Za-z]{3,}[./-]\d{4})[\s:](\d{1,2}:\d{1,2}:\d{1,2})?`

	// Regular expression3(time).
	// Eg:
	// 11:50:28
	// 11:50:28.897
	timeRegexPattern3 = `\d{1,2}:\d{1,2}:\d{1,2}(.\d{3})?`
)

func Load(name string) (*time.Location, error) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	if tz, found := cache.items[name]; found {
		return tz, nil
	}
	tz, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}
	cache.items[name] = tz
	return tz, nil
}

func ConvToInt64(v interface{}) int64 {
	switch s := v.(type) {
	case int:
		return int64(s)
	case int8:
		return int64(s)
	case int16:
		return int64(s)
	case int32:
		return int64(s)
	case int64:
		return s
	case uint:
		return int64(s)
	case uint8:
		return int64(s)
	case uint16:
		return int64(s)
	case uint32:
		return int64(s)
	case uint64:
		return int64(s)
	case string:
		timestamp, _ := strconv.ParseInt(s, 10, 64)
		return timestamp
	case *string:
		if s == nil {
			return 0
		}
		timestamp, _ := strconv.ParseInt(*s, 10, 64)
		return timestamp
	default:
		return 0
	}
}

func StrToTime(str string) (*time.Time, error) {
	result := timeRegex1.FindStringSubmatch(str)
	if len(result) != 0 && len(result[0]) != 0 {
		// TODO this will do data conversion
	}
	result = timeRegex2.FindStringSubmatch(str)
	if len(result) != 0 && len(result[0]) != 0 {
		// TODO this will do data conversion
	}
	result = timeRegex3.FindStringSubmatch(str)
	if len(result) != 0 && len(result[0]) != 0 {
		// TODO this will do data conversion
	}
	return nil, errors.New("unsupported dateTime type")
}
