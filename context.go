package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/lib/proto"
)

var _contextOptionKey = &QueryOptions{
	settings: Settings{
		"_contextOption": struct{}{},
	},
}

type QueryOption func(*QueryOptions) error

type QueryOptions struct {
	queryID  string
	quotaKey string
	progress chan Progress
	settings Settings
}

func (o *QueryOptions) Settings() []proto.Setting {
	settings := make([]proto.Setting, 0, len(o.settings))
	for k, v := range o.settings {
		settings = append(settings, proto.Setting{
			Key:   k,
			Value: fmt.Sprint(v),
		})
	}
	return settings
}

type Settings map[string]interface{}

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

func WithSettings(settings Settings) QueryOption {
	return func(o *QueryOptions) error {
		o.settings = settings
		return nil
	}
}

func WithProgress(progress chan Progress) QueryOption {
	return func(o *QueryOptions) error {
		o.progress = progress
		return nil
	}
}

func WithExternalTable() QueryOption {
	return func(o *QueryOptions) error {

		return nil
	}
}

func ExternalTable(name string, columns ...Type) {

}

type Type interface{}
