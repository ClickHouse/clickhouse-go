package clickhouse

import (
	"context"
)

func (c *connect) asyncInsert(ctx context.Context, query string, wait bool) error {
	options := queryOptions(ctx)
	{
		options.settings["async_insert"] = 1
		options.settings["wait_for_async_insert"] = 0
		if wait {
			options.settings["wait_for_async_insert"] = 1
		}
	}
	if c.err = c.sendQuery(query, &options); c.err != nil {
		return c.err
	}
	return c.process(context.Background(), options.onProcess())
}
