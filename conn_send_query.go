package clickhouse

import (
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// Connection::sendQuery
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Client/Connection.cpp
func (c *connect) sendQuery(body string, o *QueryOptions) error {
	return c.sendQueryWithDataEncoding(body, o, proto.DataEncodingNative)
}

// sendQueryWithDataEncoding sends a query whose result or INSERT data is
// exchanged in the given data encoding (one of proto.DataEncoding*).
func (c *connect) sendQueryWithDataEncoding(body string, o *QueryOptions, dataEncoding uint64) error {
	c.logger.Debug("sending query",
		slog.String("compression", c.compression.String()),
		slog.String("query", body))
	// Below DBMS_MIN_PROTOCOL_VERSION_WITH_FORMATTED_DATA the Query packet has
	// no data encoding field: formatted data uses the compatibility packet.
	packet := byte(proto.ClientQuery)
	if dataEncoding != proto.DataEncodingNative && c.revision < proto.DBMS_MIN_PROTOCOL_VERSION_WITH_FORMATTED_DATA {
		packet = proto.ClientQueryWithFormattedData
	}
	c.buffer.PutByte(packet)
	q := proto.Query{
		ClientTCPProtocolVersion: ClientTCPProtocolVersion,
		ClientName:               c.opt.ClientInfo.Append(o.clientInfo).String(),
		ClientVersion:            proto.Version{ClientVersionMajor, ClientVersionMinor, ClientVersionPatch}, //nolint:govet
		ID:                       o.queryID,
		Body:                     body,
		Span:                     o.span,
		QuotaKey:                 o.quotaKey,
		Compression:              c.compression != CompressionNone,
		InitialAddress:           c.conn.LocalAddr().String(),
		Settings:                 c.settings(o.settings),
		Parameters:               parametersToProtoParameters(o.parameters),
		DataEncoding:             dataEncoding,
	}
	if err := q.Encode(c.buffer, c.revision); err != nil {
		return err
	}
	for _, table := range o.external {
		if err := c.sendData(table.Block(), table.Name()); err != nil {
			return err
		}
	}
	if err := c.sendData(proto.NewBlock(), ""); err != nil {
		return err
	}
	return c.flush()
}

func parametersToProtoParameters(parameters Parameters) (s proto.Parameters) {
	for k, v := range parameters {
		s = append(s, proto.Parameter{
			Key:   k,
			Value: v,
		})
	}

	return s
}
