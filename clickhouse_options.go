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

package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/compress"
)

var CompressionLZ4 compress.Method = compress.LZ4

type Auth struct { // has_control_character
	Database string
	Username string
	Password string
}

type Compression struct {
	Method compress.Method
}

type ConnOpenStrategy uint8

const (
	ConnOpenInOrder ConnOpenStrategy = iota
	ConnOpenRoundRobin
)

type InterfaceType int

const (
	NativeInterface InterfaceType = iota
	HttpInterface
)

func ParseDSN(dsn string) (*Options, error) {
	opt := &Options{}
	if err := opt.fromDSN(dsn); err != nil {
		return nil, err
	}
	return opt, nil
}

type Options struct {
	Interface InterfaceType

	TLS              *tls.Config
	Addr             []string
	Auth             Auth
	DialContext      func(ctx context.Context, addr string) (net.Conn, error)
	Debug            bool
	Debugf           func(format string, v ...interface{}) // only works when Debug is true
	Settings         Settings
	Compression      *Compression
	DialTimeout      time.Duration // default 1 second
	MaxOpenConns     int           // default MaxIdleConns + 5
	MaxIdleConns     int           // default 5
	ConnMaxLifetime  time.Duration // default 1 hour
	ConnOpenStrategy ConnOpenStrategy

	ReadTimeout time.Duration
}

func (o *Options) fromDSN(in string) error {
	dsn, err := url.Parse(in)
	if err != nil {
		return err
	}
	if o.Settings == nil {
		o.Settings = make(Settings)
	}
	if dsn.User != nil {
		o.Auth.Username = dsn.User.Username()
		o.Auth.Password, _ = dsn.User.Password()
	}
	o.Addr = append(o.Addr, strings.Split(dsn.Host, ",")...)
	var (
		secure     bool
		params     = dsn.Query()
		skipVerify bool
	)
	o.Auth.Database = strings.TrimPrefix(dsn.Path, "/")
	for v := range params {
		switch v {
		case "debug":
			o.Debug, _ = strconv.ParseBool(params.Get(v))
		case "compress":
			if on, _ := strconv.ParseBool(params.Get(v)); on {
				o.Compression = &Compression{
					Method: CompressionLZ4,
				}
			}
		case "dial_timeout":
			duration, err := time.ParseDuration(params.Get(v))
			if err != nil {
				return fmt.Errorf("clickhouse [dsn parse]: dial timeout: %s", err)
			}
			o.DialTimeout = duration
		case "read_timeout":
			duration, err := time.ParseDuration(params.Get(v))
			if err != nil {
				return fmt.Errorf("clickhouse [dsn parse]: http timeout: %s", err)
			}
			o.ReadTimeout = duration
		case "secure":
			secure = true
		case "skip_verify":
			skipVerify = true
		case "connection_open_strategy":
			switch params.Get(v) {
			case "in_order":
				o.ConnOpenStrategy = ConnOpenInOrder
			case "round_robin":
				o.ConnOpenStrategy = ConnOpenRoundRobin
			}

		default:
			switch p := strings.ToLower(params.Get(v)); p {
			case "true":
				o.Settings[v] = int(1)
			case "false":
				o.Settings[v] = int(0)
			default:
				if n, err := strconv.Atoi(p); err == nil {
					o.Settings[v] = n
				}
			}
		}
	}
	if secure {
		o.TLS = &tls.Config{
			InsecureSkipVerify: skipVerify,
		}
	}
	switch dsn.Scheme {
	case "http":
		if secure {
			o.TLS = nil
		}
		o.Interface = HttpInterface
	case "https":
		if !secure {
			return fmt.Errorf("clickhouse [dsn parse]: https without TLS specify")
		}
		o.Interface = HttpInterface
	default:
		o.Interface = NativeInterface
	}
	return nil
}

func (o *Options) setDefaults() {
	if len(o.Auth.Database) == 0 {
		o.Auth.Database = "default"
	}
	if len(o.Auth.Username) == 0 {
		o.Auth.Username = "default"
	}
	if o.DialTimeout == 0 {
		o.DialTimeout = time.Second
	}
	if o.MaxIdleConns <= 0 {
		o.MaxIdleConns = 5
	}
	if o.MaxOpenConns <= 0 {
		o.MaxOpenConns = o.MaxIdleConns + 5
	}
	if o.ConnMaxLifetime == 0 {
		o.ConnMaxLifetime = time.Hour
	}
}
