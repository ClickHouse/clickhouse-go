package clickhouse

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/external"
	"go.opentelemetry.io/otel/trace"
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
		span     trace.SpanContext
		queryID  string
		quotaKey string
		events   struct {
			logs          func(*Log)
			progress      func(*Progress)
			profileInfo   func(*ProfileInfo)
			profileEvents func([]ProfileEvent)
		}
		settings Settings
		external []*external.Table
	}
)

func WithSpan(span trace.SpanContext) QueryOption {
	return func(o *QueryOptions) error {
		o.span = span
		return nil
	}
}

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

func WithLogs(fn func(*Log)) QueryOption {
	return func(o *QueryOptions) error {
		o.events.logs = fn
		return nil
	}
}

func WithProgress(fn func(*Progress)) QueryOption {
	return func(o *QueryOptions) error {
		o.events.progress = fn
		return nil
	}
}

func WithPofileInfo(fn func(*ProfileInfo)) QueryOption {
	return func(o *QueryOptions) error {
		o.events.profileInfo = fn
		return nil
	}
}

func WithProfileEvents(fn func([]ProfileEvent)) QueryOption {
	return func(o *QueryOptions) error {
		o.events.profileEvents = fn
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

func (q *QueryOptions) onProcess() *onProcess {
	return &onProcess{
		logs: func(logs []Log) {
			if q.events.logs != nil {
				for _, l := range logs {
					q.events.logs(&l)
				}
			}
		},
		progress: func(p *Progress) {
			if q.events.progress != nil {
				q.events.progress(p)
			}
		},
		profileInfo: func(p *ProfileInfo) {
			if q.events.profileInfo != nil {
				q.events.profileInfo(p)
			}
		},
		profileEvents: func(events []ProfileEvent) {
			if q.events.profileEvents != nil {
				q.events.profileEvents(events)
			}
		},
	}
}
