package proto

import (
	"os"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

var (
	osUser      = os.Getenv("USER")
	hostname, _ = os.Hostname()
)

type Query struct {
	ID             string
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
		encoder.Byte(0)
		/*
					 // Have OpenTelemetry header.
			            writeBinary(uint8_t(1), out);
			            // No point writing these numbers with variable length, because they
			            // are random and will probably require the full length anyway.
			            writeBinary(client_trace_context.trace_id, out);
			            writeBinary(client_trace_context.span_id, out);
			            writeBinary(client_trace_context.tracestate, out);
			            writeBinary(client_trace_context.trace_flags, out);
		*/
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
	Value string
}

func (s Settings) Encode(encoder *binary.Encoder, revision uint64) error {
	if revision <= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
		return nil
	}
	for _, s := range s {
		if err := s.encode(encoder); err != nil {
			return err
		}
	}
	return nil
}

func (s *Setting) encode(encoder *binary.Encoder) error {
	encoder.String(s.Key)
	encoder.Bool(true) // is_important
	return encoder.String(s.Value)
}
