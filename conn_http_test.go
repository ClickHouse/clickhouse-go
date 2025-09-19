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
