package clickhouse

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func (c *connect) asyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	options := queryOptions(ctx)
	options.applyAsyncInsertSettings(wait)

	if len(args) > 0 {
		queryParamsProtocolSupport := c.revision >= proto.DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS
		var err error
		query, err = bindQueryOrAppendParameters(queryParamsProtocolSupport, &options, query, c.server.Timezone, args...)
		if err != nil {
			return err
		}
	}

	if err := c.sendQuery(query, &options); err != nil {
		return err
	}
	return c.process(ctx, options.onProcess())
}
