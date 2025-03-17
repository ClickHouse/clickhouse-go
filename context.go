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
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"go.opentelemetry.io/otel/trace"
)

// contextKey is a custom type to avoid key collisions in context
type contextKey string

// Use a string as a context key to avoid lock copying issues
var _contextOptionKey = contextKey("clickhouse_query_options")

// CustomSetting is a helper struct to distinguish custom settings from important ones.
// For native protocol, is_important flag is set to value 0x02 (see https://github.com/ClickHouse/ClickHouse/blob/c873560fe7185f45eed56520ec7d033a7beb1551/src/Core/BaseSettings.h#L516-L521)
// Only string value is supported until formatting logic that exists in ClickHouse is implemented in clickhouse-go. (https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Field.cpp#L312 and https://github.com/ClickHouse/clickhouse-go/issues/992)
type CustomSetting struct {
	Value string
}

type Settings map[string]any

type Parameters map[string]string

// QueryOption is a function that configures a QueryOptions object
type QueryOption func(*QueryOptions) error

// QueryOptions holds all query-specific options and settings
type QueryOptions struct {
	mu               sync.RWMutex // Protects settings and parameters
	span             trace.SpanContext
	async            struct {
		ok   bool
		wait bool
	}
	queryID          string
	quotaKey         string
	events           struct {
		logs          func(*Log)
		progress      func(*Progress)
		profileInfo   func(*ProfileInfo)
		profileEvents func([]ProfileEvent)
	}
	settings         Settings
	parameters       Parameters
	external         []*ext.Table
	blockBufferSize  uint8
	userLocation     *time.Location
}

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

func WithBlockBufferSize(size uint8) QueryOption {
	return func(o *QueryOptions) error {
		o.blockBufferSize = size
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
		o.mu.Lock()
		defer o.mu.Unlock()
		o.settings = settings
		return nil
	}
}

func WithParameters(params Parameters) QueryOption {
	return func(o *QueryOptions) error {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.parameters = params
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

func WithProfileInfo(fn func(*ProfileInfo)) QueryOption {
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

func WithExternalTable(t ...*ext.Table) QueryOption {
	return func(o *QueryOptions) error {
		o.external = append(o.external, t...)
		return nil
	}
}

func WithStdAsync(wait bool) QueryOption {
	return func(o *QueryOptions) error {
		o.async.ok, o.async.wait = true, wait
		return nil
	}
}

func WithUserLocation(location *time.Location) QueryOption {
	return func(o *QueryOptions) error {
		o.userLocation = location
		return nil
	}
}

func ignoreExternalTables() QueryOption {
	return func(o *QueryOptions) error {
		o.external = nil
		return nil
	}
}

func Context(parent context.Context, options ...QueryOption) context.Context {
	opt := queryOptions(parent)
	for _, f := range options {
		f(opt)
	}
	return context.WithValue(parent, _contextOptionKey, opt)
}

func queryOptions(ctx context.Context) *QueryOptions {
	if value := ctx.Value(_contextOptionKey); value != nil {
		if o, ok := value.(*QueryOptions); ok && o != nil {
			// Create a new instance to avoid lock copying
			newOpt := &QueryOptions{
				span:     o.span,
				async:    o.async,
				queryID:  o.queryID,
				quotaKey: o.quotaKey,
				events:   o.events,
				external: o.external,
				blockBufferSize: o.blockBufferSize,
				userLocation:    o.userLocation,
			}
			
			// Copy settings and parameters with mutex protection
			o.mu.RLock()
			newOpt.settings = make(Settings, len(o.settings))
			for k, v := range o.settings {
				newOpt.settings[k] = v
			}
			
			if o.parameters != nil {
				newOpt.parameters = make(Parameters, len(o.parameters))
				for k, v := range o.parameters {
					newOpt.parameters[k] = v
				}
			}
			o.mu.RUnlock()
			
			// Check if we need to adjust execution time based on deadline
			if deadline, ok := ctx.Deadline(); ok {
				if sec := time.Until(deadline).Seconds(); sec > 1 {
					newOpt.settings["max_execution_time"] = int(sec + 5)
				}
			}
			
			return newOpt
		}
	}
	
	return &QueryOptions{
		settings: make(Settings),
	}
}

// GetSettings returns a copy of the settings map guarded by RWMutex
func (q *QueryOptions) GetSettings() Settings {
	q.mu.RLock()
	defer q.mu.RUnlock()
	
	// Create a copy to avoid race conditions
	settingsCopy := make(Settings, len(q.settings))
	for k, v := range q.settings {
		settingsCopy[k] = v
	}
	return settingsCopy
}

// GetParameters returns a copy of the parameters map guarded by RWMutex
func (q *QueryOptions) GetParameters() Parameters {
	q.mu.RLock()
	defer q.mu.RUnlock()
	
	// Create a copy to avoid race conditions
	paramsCopy := make(Parameters, len(q.parameters))
	for k, v := range q.parameters {
		paramsCopy[k] = v
	}
	return paramsCopy
}

// SetSetting updates a single setting in a thread-safe way
func (q *QueryOptions) SetSetting(key string, value any) {
	q.mu.Lock()
	defer q.mu.Unlock()
	
	if q.settings == nil {
		q.settings = make(Settings)
	}
	q.settings[key] = value
}

// SetParameter updates a single parameter in a thread-safe way
func (q *QueryOptions) SetParameter(key, value string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	
	if q.parameters == nil {
		q.parameters = make(Parameters)
	}
	q.parameters[key] = value
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
