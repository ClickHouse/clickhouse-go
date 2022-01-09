package clickhouse

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/external"
)

var _contextOptionKey = &QueryOptions{
	settings: Settings{
		"_contextOption": struct{}{},
	},
}

type Settings map[string]interface{}
type (
	QueryOption  func(*QueryOptions) error
	QueryOptions struct {
		queryID  string
		quotaKey string
		events   struct {
			logs          chan Log
			progress      chan Progress
			profileEvents chan ProfileEvent
		}
		settings Settings
		external []*external.Table
	}
)

func WithQueryID(queryID string) QueryOption {
	return func(o *QueryOptions) error {
		o.queryID = queryID
		return nil
	}
}

func WithQuotaKey(quotaKey string) QueryOption {
	return func(o *QueryOptions) error {
		o.quotaKey = quotaKey
		return nil
	}
}

func WithSettings(settings Settings) QueryOption {
	return func(o *QueryOptions) error {
		o.settings = settings
		return nil
	}
}

func WithProgress(progress chan Progress) QueryOption {
	return func(o *QueryOptions) error {
		o.events.progress = progress
		return nil
	}
}

func WithLogs(logs chan Log) QueryOption {
	return func(o *QueryOptions) error {
		o.events.logs = logs
		return nil
	}
}

func WithProfileEvents(events chan ProfileEvent) QueryOption {
	return func(o *QueryOptions) error {
		o.events.profileEvents = events
		return nil
	}
}

func WithExternalTable(t ...*external.Table) QueryOption {
	return func(o *QueryOptions) error {
		o.external = append(o.external, t...)
		return nil
	}
}

func Context(parent context.Context, options ...QueryOption) context.Context {
	var opt QueryOptions
	for _, f := range options {
		f(&opt)
	}
	return context.WithValue(parent, _contextOptionKey, opt)
}

func queryOptions(ctx context.Context) QueryOptions {
	if o, ok := ctx.Value(_contextOptionKey).(QueryOptions); ok {
		return o
	}
	return QueryOptions{}
}
