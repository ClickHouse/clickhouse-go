package clickhouse

import (
	"context"
	"net/http"
	"testing"
)

func TestCreateHTTPRoundTripper(t *testing.T) {
	transportFnCalled := false
	_, err := createHTTPRoundTripper(&Options{
		TransportFunc: func(t *http.Transport) (http.RoundTripper, error) {
			transportFnCalled = true
			return t, nil
		},
	})
	if err != nil {
		t.Fatalf("can not set up client: %s", err)
	}
	if !transportFnCalled {
		t.Fatal("TransportFn not called")
	}
}

func TestApplyOptionsToRequest_HostHeader(t *testing.T) {
	tests := []struct {
		name            string
		headers         map[string]string
		expectedHost    string
		expectedInMap   map[string]string
		unexpectedInMap []string
	}{
		{
			name:            "Host header sets req.Host",
			headers:         map[string]string{"Host": "my-service.example.com"},
			expectedHost:    "my-service.example.com",
			unexpectedInMap: []string{"Host"},
		},
		{
			name:            "lowercase host header sets req.Host",
			headers:         map[string]string{"host": "my-service.example.com"},
			expectedHost:    "my-service.example.com",
			unexpectedInMap: []string{"Host"},
		},
		{
			name:            "uppercase HOST header sets req.Host",
			headers:         map[string]string{"HOST": "my-service.example.com"},
			expectedHost:    "my-service.example.com",
			unexpectedInMap: []string{"Host"},
		},
		{
			name: "Host header with other headers",
			headers: map[string]string{
				"Host":           "my-service.example.com",
				"X-Custom-Token": "abc123",
			},
			expectedHost:    "my-service.example.com",
			expectedInMap:   map[string]string{"X-Custom-Token": "abc123"},
			unexpectedInMap: []string{"Host"},
		},
		{
			name:          "no Host header leaves req.Host unchanged",
			headers:       map[string]string{"X-Custom": "value"},
			expectedHost:  "localhost:8123",
			expectedInMap: map[string]string{"X-Custom": "value"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, "http://localhost:8123/", nil)
			if err != nil {
				t.Fatalf("failed to create request: %s", err)
			}

			opts := &Options{
				HttpHeaders: tt.headers,
			}

			if err := applyOptionsToRequest(context.Background(), req, opts); err != nil {
				t.Fatalf("applyOptionsToRequest failed: %s", err)
			}

			if req.Host != tt.expectedHost {
				t.Errorf("req.Host = %q, want %q", req.Host, tt.expectedHost)
			}

			for k, v := range tt.expectedInMap {
				if got := req.Header.Get(k); got != v {
					t.Errorf("req.Header[%q] = %q, want %q", k, got, v)
				}
			}

			for _, k := range tt.unexpectedInMap {
				if got := req.Header.Get(k); got != "" {
					t.Errorf("req.Header[%q] = %q, want it absent", k, got)
				}
			}
		})
	}
}
