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

package proto

import (
	stdbin "encoding/binary"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"go.opentelemetry.io/otel/trace"
)

var (
	osUser      = os.Getenv("USER")
	hostname, _ = os.Hostname()
)

type Query struct {
	ID             string
	Span           trace.SpanContext
	Body           string
	QuotaKey       string
	Settings       Settings
	Compression    bool
	InitialUser    string
	InitialAddress string
}

func (q *Query) Encode(encoder *binary.Encoder, revision uint64) error {
	if err := encoder.String(q.ID); err != nil {
		return err
	}
	// client_info
	if err := q.encodeClientInfo(encoder, revision); err != nil {
		return err
	}
	// settings
	if err := q.Settings.Encode(encoder, revision); err != nil {
		return err
	}
	encoder.String("" /* empty string is a marker of the end of setting */)

	if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
		encoder.String("")
	}
	{
		encoder.Byte(StateComplete)
		encoder.Bool(q.Compression)
	}
	return encoder.String(q.Body)
}

func swap64(b []byte) {
	for i := 0; i < len(b); i += 8 {
		u := stdbin.BigEndian.Uint64(b[i:])
		stdbin.LittleEndian.PutUint64(b[i:], u)
	}
}

func (q *Query) encodeClientInfo(encoder *binary.Encoder, revision uint64) error {
	encoder.Byte(ClientQueryInitial)
	encoder.String(q.InitialUser)    // initial_user
	encoder.String("")               // initial_query_id
	encoder.String(q.InitialAddress) // initial_address
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME {
		encoder.Int64(0) // initial_query_start_time_microseconds
	}
	encoder.Byte(1) // interface [tcp - 1, http - 2]
	{
		encoder.String(osUser)
		encoder.String(hostname)
		encoder.String(ClientName)
		encoder.Uvarint(ClientVersionMajor)
		encoder.Uvarint(ClientVersionMinor)
		encoder.Uvarint(ClientTCPProtocolVersion)
	}
	if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		encoder.String(q.QuotaKey)
	}
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
		encoder.Uvarint(0)
	}
	if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		encoder.Uvarint(0)
	}
	if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
		switch {
		case q.Span.IsValid():
			encoder.Byte(1)
			{
				v := q.Span.TraceID()
				swap64(v[:]) // https://github.com/ClickHouse/ClickHouse/issues/34369
				encoder.Raw(v[:])
			}
			{
				v := q.Span.SpanID()
				swap64(v[:]) // https://github.com/ClickHouse/ClickHouse/issues/34369
				encoder.Raw(v[:])
			}
			encoder.String(q.Span.TraceState().String())
			encoder.Byte(byte(q.Span.TraceFlags()))

		default:
			encoder.Byte(0)
		}
	}
	if revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
		encoder.Uvarint(0) // collaborate_with_initiator
		encoder.Uvarint(0) // count_participating_replicas
		encoder.Uvarint(0) // number_of_current_replica
	}
	return nil
}

type Settings []Setting

type Setting struct {
	Key   string
	Value interface{}
}

func (s Settings) Encode(encoder *binary.Encoder, revision uint64) error {
	for _, s := range s {
		if err := s.encode(encoder, revision); err != nil {
			return err
		}
	}
	return nil
}

func (s *Setting) encode(encoder *binary.Encoder, revision uint64) error {
	if err := encoder.String(s.Key); err != nil {
		return err
	}
	if revision <= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
		var value uint64
		switch v := s.Value.(type) {
		case int:
			value = uint64(v)
		case bool:
			if value = 0; v {
				value = 1
			}
		default:
			return fmt.Errorf("query setting %s has unsupported data type", s.Key)
		}
		return encoder.Uvarint(value)
	}
	if err := encoder.Bool(true); err != nil { // is_important
		return err
	}
	return encoder.String(fmt.Sprint(s.Value))
}
