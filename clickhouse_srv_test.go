package clickhouse

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSrvLookupName(t *testing.T) {
	testCases := []struct {
		name           string
		addr           string
		opt            *Options
		wantService    string
		wantProto      string
		wantLookupName string
	}{
		{
			name:           "plain host, native",
			addr:           "example.com",
			opt:            &Options{Protocol: Native},
			wantService:    "clickhouse",
			wantProto:      "tcp",
			wantLookupName: "example.com",
		},
		{
			name:           "host with port, native",
			addr:           "example.com:9000",
			opt:            &Options{Protocol: Native},
			wantService:    "clickhouse",
			wantProto:      "tcp",
			wantLookupName: "example.com",
		},
		{
			name:           "full SRV name starting with underscore",
			addr:           "_clickhouse._tcp.example.com",
			opt:            &Options{Protocol: Native},
			wantService:    "",
			wantProto:      "",
			wantLookupName: "_clickhouse._tcp.example.com",
		},
		{
			name:           "HTTP without TLS",
			addr:           "example.com",
			opt:            &Options{Protocol: HTTP},
			wantService:    "clickhouse-http",
			wantProto:      "tcp",
			wantLookupName: "example.com",
		},
		{
			name:           "HTTP with TLS",
			addr:           "example.com:8443",
			opt:            &Options{Protocol: HTTP, TLS: &tls.Config{}},
			wantService:    "clickhouse-https",
			wantProto:      "tcp",
			wantLookupName: "example.com",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc, proto, name := srvLookupName(tc.addr, tc.opt)
			assert.Equal(t, tc.wantService, svc)
			assert.Equal(t, tc.wantProto, proto)
			assert.Equal(t, tc.wantLookupName, name)
		})
	}
}

func TestResolveAddrDisabled(t *testing.T) {
	called := false
	opt := &Options{
		Addr: []string{"example.com:9000", "other:9000"},
		lookupSRV: func(context.Context, string, string, string) (string, []*net.SRV, error) {
			called = true
			return "", nil, nil
		},
	}
	got, err := resolveAddr(context.Background(), opt)
	require.NoError(t, err)
	assert.Equal(t, opt.Addr, got)
	assert.False(t, called, "lookupSRV must not be called when ResolveSRV is false")
}

func TestResolveAddrOrdering(t *testing.T) {
	opt := &Options{
		Protocol:   Native,
		Addr:       []string{"example.com"},
		ResolveSRV: true,
		lookupSRV: func(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
			require.Equal(t, "clickhouse", service)
			require.Equal(t, "tcp", proto)
			require.Equal(t, "example.com", name)
			return "", []*net.SRV{
				{Target: "c.example.com.", Port: 9000, Priority: 20, Weight: 10},
				{Target: "a.example.com.", Port: 9000, Priority: 10, Weight: 5},
				{Target: "b.example.com.", Port: 9000, Priority: 10, Weight: 50},
				{Target: "d.example.com.", Port: 9000, Priority: 20, Weight: 100},
			}, nil
		},
	}
	got, err := resolveAddr(context.Background(), opt)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"b.example.com:9000",
		"a.example.com:9000",
		"d.example.com:9000",
		"c.example.com:9000",
	}, got)
}

func TestResolveAddrConcatenation(t *testing.T) {
	opt := &Options{
		Protocol:   Native,
		Addr:       []string{"one.example.com", "two.example.com"},
		ResolveSRV: true,
		lookupSRV: func(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
			switch name {
			case "one.example.com":
				return "", []*net.SRV{{Target: "a.", Port: 9000, Priority: 10, Weight: 1}}, nil
			case "two.example.com":
				return "", []*net.SRV{{Target: "b.", Port: 9001, Priority: 10, Weight: 1}}, nil
			}
			t.Fatalf("unexpected lookup for %q", name)
			return "", nil, nil
		},
	}
	got, err := resolveAddr(context.Background(), opt)
	require.NoError(t, err)
	assert.Equal(t, []string{"a:9000", "b:9001"}, got)
}

func TestResolveAddrFullName(t *testing.T) {
	opt := &Options{
		Protocol:   Native,
		Addr:       []string{"_clickhouse._tcp.example.com"},
		ResolveSRV: true,
		lookupSRV: func(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
			assert.Equal(t, "", service)
			assert.Equal(t, "", proto)
			assert.Equal(t, "_clickhouse._tcp.example.com", name)
			return "", []*net.SRV{{Target: "node1.example.com.", Port: 9000}}, nil
		},
	}
	got, err := resolveAddr(context.Background(), opt)
	require.NoError(t, err)
	assert.Equal(t, []string{"node1.example.com:9000"}, got)
}

func TestResolveAddrErrorWrapping(t *testing.T) {
	sentinel := errors.New("boom")
	opt := &Options{
		Protocol:   Native,
		Addr:       []string{"example.com"},
		ResolveSRV: true,
		lookupSRV: func(context.Context, string, string, string) (string, []*net.SRV, error) {
			return "", nil, sentinel
		},
	}
	_, err := resolveAddr(context.Background(), opt)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.Contains(t, err.Error(), `resolve SRV "example.com"`)
}

func TestResolveAddrEmpty(t *testing.T) {
	opt := &Options{
		Protocol:   Native,
		Addr:       []string{"example.com"},
		ResolveSRV: true,
		lookupSRV: func(context.Context, string, string, string) (string, []*net.SRV, error) {
			return "", nil, nil
		},
	}
	_, err := resolveAddr(context.Background(), opt)
	assert.ErrorIs(t, err, ErrAcquireConnNoAddress)
}

func TestResolveAddrSkipsRootTarget(t *testing.T) {
	newOpt := func(records []*net.SRV) *Options {
		return &Options{
			Protocol:   Native,
			Addr:       []string{"example.com"},
			ResolveSRV: true,
			lookupSRV: func(context.Context, string, string, string) (string, []*net.SRV, error) {
				return "", records, nil
			},
		}
	}

	// RFC 2782: a target of "." means the service is not available.
	_, err := resolveAddr(context.Background(), newOpt([]*net.SRV{{Target: ".", Port: 9000}}))
	assert.ErrorIs(t, err, ErrAcquireConnNoAddress)

	got, err := resolveAddr(context.Background(), newOpt([]*net.SRV{
		{Target: ".", Port: 9000},
		{Target: "a.example.com.", Port: 9000},
	}))
	require.NoError(t, err)
	assert.Equal(t, []string{"a.example.com:9000"}, got)
}

func TestClickhouseDialResolvesSRV(t *testing.T) {
	var gotAddrs []string
	resolved := []*net.SRV{
		{Target: "resolved.example.com.", Port: 9000, Priority: 10, Weight: 1},
	}
	opt := (&Options{
		Protocol:   Native,
		Addr:       []string{"example.com"},
		ResolveSRV: true,
		lookupSRV: func(context.Context, string, string, string) (string, []*net.SRV, error) {
			return "", resolved, nil
		},
		DialStrategy: func(ctx context.Context, connID int, o *Options, dial Dial) (DialResult, error) {
			gotAddrs = append([]string(nil), o.Addr...)
			return DialResult{}, errors.New("stop")
		},
	}).setDefaults()

	ch := &clickhouse{opt: opt}
	_, err := ch.dial(context.Background())
	require.Error(t, err)
	assert.Equal(t, []string{"resolved.example.com:9000"}, gotAddrs)
	// Original Addr must not be mutated.
	assert.Equal(t, []string{"example.com"}, opt.Addr)
}

func TestStdConnectSRVLookupBoundedByDialTimeout(t *testing.T) {
	opt := &Options{
		Addr:        []string{"db.example.com"},
		ResolveSRV:  true,
		DialTimeout: 50 * time.Millisecond,
		lookupSRV: func(ctx context.Context, _, _, _ string) (string, []*net.SRV, error) {
			<-ctx.Done()
			return "", nil, ctx.Err()
		},
	}
	o := &stdConnOpener{opt: opt, logger: slog.New(slog.DiscardHandler)}

	start := time.Now()
	_, err := o.Connect(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 5*time.Second)
}
