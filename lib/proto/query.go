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
	"strings"

	chproto "github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/otel/trace"
)

var (
	osUser      = os.Getenv("USER")
	hostname, _ = os.Hostname()
)

type Query struct {
	ID                       string
	ClientName               string
	ClientVersion            Version
	ClientTCPProtocolVersion uint64
	Span                     trace.SpanContext
	Body                     string
	QuotaKey                 string
	Settings                 Settings
	Parameters               Parameters
	Compression              bool
	InitialUser              string
	InitialAddress           string
}

func (q *Query) Encode(buffer *chproto.Buffer, revision uint64) error {
	buffer.PutString(q.ID)
	// client_info
	if err := q.encodeClientInfo(buffer, revision); err != nil {
		return err
	}
	// settings
	if err := q.Settings.Encode(buffer, revision); err != nil {
		return err
	}
	buffer.PutString("") /* empty string is a marker of the end of setting */

	if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
		buffer.PutString("")
	}
	{
		buffer.PutByte(StateComplete)
		buffer.PutBool(q.Compression)
	}
	buffer.PutString(q.Body)

	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
		if err := q.Parameters.Encode(buffer, revision); err != nil {
			return err
		}
		buffer.PutString("") /* empty string is a marker of the end of parameters */
	}

	return nil
}

func swap64(b []byte) {
	for i := 0; i < len(b); i += 8 {
		u := stdbin.BigEndian.Uint64(b[i:])
		stdbin.LittleEndian.PutUint64(b[i:], u)
	}
}

func (q *Query) encodeClientInfo(buffer *chproto.Buffer, revision uint64) error {
	buffer.PutByte(ClientQueryInitial)
	buffer.PutString(q.InitialUser)    // initial_user
	buffer.PutString("")               // initial_query_id
	buffer.PutString(q.InitialAddress) // initial_address
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME {
		buffer.PutInt64(0) // initial_query_start_time_microseconds
	}
	buffer.PutByte(1) // interface [tcp - 1, http - 2]
	{
		buffer.PutString(osUser)
		buffer.PutString(hostname)
		buffer.PutString(q.ClientName)
		buffer.PutUVarInt(q.ClientVersion.Major)
		buffer.PutUVarInt(q.ClientVersion.Minor)
		buffer.PutUVarInt(q.ClientTCPProtocolVersion)
	}
	if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		buffer.PutString(q.QuotaKey)
	}
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
		buffer.PutUVarInt(0)
	}
	if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		buffer.PutUVarInt(0)
	}
	if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
		switch {
		case q.Span.IsValid():
			buffer.PutByte(1)
			{
				v := q.Span.TraceID()
				swap64(v[:]) // https://github.com/ClickHouse/ClickHouse/issues/34369
				buffer.PutRaw(v[:])
			}
			{
				v := q.Span.SpanID()
				swap64(v[:]) // https://github.com/ClickHouse/ClickHouse/issues/34369
				buffer.PutRaw(v[:])
			}
			buffer.PutString(q.Span.TraceState().String())
			buffer.PutByte(byte(q.Span.TraceFlags()))

		default:
			buffer.PutByte(0)
		}
	}
	if revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
		buffer.PutUVarInt(0) // collaborate_with_initiator
		buffer.PutUVarInt(0) // count_participating_replicas
		buffer.PutUVarInt(0) // number_of_current_replica
	}
	return nil
}

func (q *Query) Decode(reader *chproto.Reader, revision uint64) error {
	// Read query ID
	var err error
	if q.ID, err = reader.Str(); err != nil {
		return fmt.Errorf("could not read query ID: %v", err)
	}

	// Decode client info
	if err := q.decodeClientInfo(reader, revision); err != nil {
		return err
	}

	// Decode settings
	if err := q.Settings.Decode(reader, revision); err != nil {
		return err
	}

	// Read interserver secret (if supported)
	if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
		if _, err := reader.Str(); err != nil {
			return fmt.Errorf("could not read interserver secret: %v", err)
		}
	}

	// Read stage and compression
	stage, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("could not read query stage: %v", err)
	}
	_ = stage // StateComplete is expected

	if q.Compression, err = reader.Bool(); err != nil {
		return fmt.Errorf("could not read compression flag: %v", err)
	}

	// Read query body
	if q.Body, err = reader.Str(); err != nil {
		return fmt.Errorf("could not read query body: %v", err)
	}

	// Read parameters (if supported)
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
		if err := q.Parameters.Decode(reader, revision); err != nil {
			return err
		}

		// Read empty string marker for end of parameters
		if _, err := reader.Str(); err != nil {
			return fmt.Errorf("could not read parameters end marker: %v", err)
		}
	}

	return nil
}

func (q *Query) decodeClientInfo(reader *chproto.Reader, revision uint64) error {
	// Read client query type
	queryType, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("could not read client query type: %v", err)
	}
	_ = queryType // ClientQueryInitial is expected

	// Read initial user
	if q.InitialUser, err = reader.Str(); err != nil {
		return fmt.Errorf("could not read initial user: %v", err)
	}

	// Read initial query ID (skip)
	if _, err := reader.Str(); err != nil {
		return fmt.Errorf("could not read initial query ID: %v", err)
	}

	// Read initial address
	if q.InitialAddress, err = reader.Str(); err != nil {
		return fmt.Errorf("could not read initial address: %v", err)
	}

	// Read initial query start time (if supported)
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME {
		_, err := reader.Int64()
		if err != nil {
			return fmt.Errorf("could not read initial query start time: %v", err)
		}
	}

	// Read interface type
	if _, err := reader.ReadByte(); err != nil {
		return fmt.Errorf("could not read interface type: %v", err)
	}

	// Read OS user (skip)

	if _, err := reader.Str(); err != nil {
		return fmt.Errorf("could not read OS user: %v", err)
	}

	// Read hostname (skip)
	if _, err := reader.Str(); err != nil {
		return fmt.Errorf("could not read hostname: %v", err)
	}

	// Read client name
	if q.ClientName, err = reader.Str(); err != nil {
		return fmt.Errorf("could not read client name: %v", err)
	}

	// Read client version
	if q.ClientVersion.Major, err = reader.UVarInt(); err != nil {
		return fmt.Errorf("could not read client major version: %v", err)
	}
	if q.ClientVersion.Minor, err = reader.UVarInt(); err != nil {
		return fmt.Errorf("could not read client minor version: %v", err)
	}

	// Read client TCP protocol version
	if q.ClientTCPProtocolVersion, err = reader.UVarInt(); err != nil {
		return fmt.Errorf("could not read client TCP protocol version: %v", err)
	}

	// Read quota key (if supported)
	if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		if q.QuotaKey, err = reader.Str(); err != nil {
			return fmt.Errorf("could not read quota key: %v", err)
		}
	}

	// Read distributed depth (if supported)
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
		if _, err := reader.UVarInt(); err != nil {
			return fmt.Errorf("could not read distributed depth: %v", err)
		}
	}

	// Read version patch (if supported)
	if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		q.ClientVersion.Patch, err = reader.UVarInt()
		if err != nil {
			return fmt.Errorf("could not read version patch: %v", err)
		}
	}

	// Read OpenTelemetry trace info (if supported)
	if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
		hasTrace, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("could not read trace flag: %v", err)
		}

		if hasTrace == 1 {
			// Read trace ID
			traceIDBytes := make([]byte, 16)
			if err := reader.ReadFull(traceIDBytes); err != nil {
				return fmt.Errorf("could not read trace ID: %v", err)
			}
			swap64(traceIDBytes) // Reverse the swap done during encoding

			// Read span ID
			spanIDBytes := make([]byte, 8)
			if err := reader.ReadFull(spanIDBytes); err != nil {
				return fmt.Errorf("could not read span ID: %v", err)
			}
			swap64(spanIDBytes) // Reverse the swap done during encoding

			// Read trace state
			traceState, err := reader.Str()
			if err != nil {
				return fmt.Errorf("could not read trace state: %v", err)
			}

			// Read trace flags
			traceFlags, err := reader.ReadByte()
			if err != nil {
				return fmt.Errorf("could not read trace flags: %v", err)
			}

			// Reconstruct SpanContext (this is a simplified reconstruction)
			// In a real implementation, you'd properly reconstruct the trace.SpanContext
			// For now, we'll store the raw data or skip reconstruction
			_ = traceIDBytes
			_ = spanIDBytes
			_ = traceState
			_ = traceFlags
		}
	}

	// Read parallel replicas info (if supported)
	if revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
		// Read collaborate_with_initiator
		if _, err := reader.UVarInt(); err != nil {
			return fmt.Errorf("could not read collaborate_with_initiator: %v", err)
		}

		// Read count_participating_replicas
		if _, err := reader.UVarInt(); err != nil {
			return fmt.Errorf("could not read count_participating_replicas: %v", err)
		}

		// Read number_of_current_replica
		if _, err := reader.UVarInt(); err != nil {
			return fmt.Errorf("could not read number_of_current_replica: %v", err)
		}
	}

	return nil
}

type Settings []Setting

type Setting struct {
	Key       string
	Value     any
	Important bool
	Custom    bool
}

const (
	settingFlagImportant = 0x01
	settingFlagCustom    = 0x02
)

func (s Settings) Encode(buffer *chproto.Buffer, revision uint64) error {
	for _, s := range s {
		if err := s.encode(buffer, revision); err != nil {
			return err
		}
	}
	return nil
}

func (s *Setting) encode(buffer *chproto.Buffer, revision uint64) error {
	buffer.PutString(s.Key)
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
		buffer.PutUVarInt(value)
		return nil
	}

	{
		var flags uint64
		if s.Important {
			flags |= settingFlagImportant
		}
		if s.Custom {
			flags |= settingFlagCustom
		}
		buffer.PutUVarInt(flags)
	}

	if s.Custom {
		fieldDump, err := encodeFieldDump(s.Value)
		if err != nil {
			return err
		}

		buffer.PutString(fieldDump)
	} else {
		buffer.PutString(fmt.Sprint(s.Value))
	}

	return nil
}

func (s *Settings) Decode(reader *chproto.Reader, revision uint64) error {
	*s = (*s)[:0] // Clear existing settings

	for {
		// Read setting key
		key, err := reader.Str()
		if err != nil {
			return fmt.Errorf("could not read setting key: %v", err)
		}

		// Empty key indicates end of settings
		if key == "" {
			break
		}

		setting := Setting{Key: key}

		if revision <= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
			// Old format: value as UVarInt
			value, err := reader.UVarInt()
			if err != nil {
				return fmt.Errorf("could not read setting value: %v", err)
			}
			setting.Value = value
		} else {
			// New format: flags + string value
			flags, err := reader.UVarInt()
			if err != nil {
				return fmt.Errorf("could not read setting flags: %v", err)
			}

			setting.Important = (flags & settingFlagImportant) != 0
			setting.Custom = (flags & settingFlagCustom) != 0

			valueStr, err := reader.Str()
			if err != nil {
				return fmt.Errorf("could not read setting value: %v", err)
			}

			if setting.Custom {
				// Decode field dump
				value, err := decodeFieldDump(valueStr)
				if err != nil {
					return fmt.Errorf("could not decode field dump for setting %s: %v", key, err)
				}
				setting.Value = value
			} else {
				setting.Value = valueStr
			}
		}

		*s = append(*s, setting)
	}

	return nil
}

type Parameters []Parameter

type Parameter struct {
	Key   string
	Value string
}

func (s Parameters) Encode(buffer *chproto.Buffer, revision uint64) error {
	for _, s := range s {
		if err := s.encode(buffer, revision); err != nil {
			return err
		}
	}
	return nil
}

func (s *Parameter) encode(buffer *chproto.Buffer, revision uint64) error {
	buffer.PutString(s.Key)
	buffer.PutUVarInt(uint64(settingFlagCustom))

	fieldDump, err := encodeFieldDump(s.Value)
	if err != nil {
		return err
	}

	buffer.PutString(fieldDump)

	return nil
}

func (p *Parameters) Decode(reader *chproto.Reader, revision uint64) error {
	*p = (*p)[:0] // Clear existing parameters

	for {
		// Read parameter key
		key, err := reader.Str()
		if err != nil {
			return fmt.Errorf("could not read parameter key: %v", err)
		}

		// Empty key indicates end of parameters
		if key == "" {
			break
		}

		// Read flags (should be settingFlagCustom for parameters)
		if _, err := reader.UVarInt(); err != nil {
			return fmt.Errorf("could not read parameter flags: %v", err)
		}

		// Read parameter value
		valueStr, err := reader.Str()
		if err != nil {
			return fmt.Errorf("could not read parameter value: %v", err)
		}

		// Decode field dump
		value, err := decodeFieldDump(valueStr)
		if err != nil {
			return fmt.Errorf("could not decode field dump for parameter %s: %v", key, err)
		}

		parameter := Parameter{
			Key:   key,
			Value: value,
		}

		*p = append(*p, parameter)
	}

	return nil
}

// encodes a field dump with an appropriate type format
// implements the same logic as in ClickHouse Field::restoreFromDump (https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Field.cpp#L312)
// currently, only string type is supported
func encodeFieldDump(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%v'", strings.ReplaceAll(v, "'", "\\'")), nil
	}

	return "", fmt.Errorf("unsupported field type %T", value)
}

// decodeFieldDump decodes a field dump string back to its original value
// This reverses the encodeFieldDump function
func decodeFieldDump(dump string) (string, error) {
	// Handle string format: 'value' -> value
	if len(dump) >= 2 && dump[0] == '\'' && dump[len(dump)-1] == '\'' {
		// Remove surrounding quotes and unescape
		value := dump[1 : len(dump)-1]
		value = strings.ReplaceAll(value, "\\'", "'")
		return value, nil
	}

	// If not in string format, return as-is
	return dump, nil
}
