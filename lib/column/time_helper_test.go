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

package column

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetTimeWithDifferentLocation(t *testing.T) {
	tests := []struct {
		in   string
		loc  *time.Location
		want string
	}{
		{
			in:   "2023-02-15 14:02:12.321 +00:00",
			loc:  time.FixedZone("", 60*60),
			want: "2023-02-15 14:02:12.321 +01:00",
		},
		{
			in:   "2023-02-15 14:02:12.321 +03:00",
			loc:  time.FixedZone("", -4*60*60),
			want: "2023-02-15 14:02:12.321 -04:00",
		},
		{
			in:   "2024-02-29 02:01:12 -06:00",
			loc:  time.FixedZone("", -4*60*60),
			want: "2024-02-29 02:01:12 -04:00",
		},
		{
			in:   "2023-02-15 04:02:12.321 +02:00",
			loc:  time.UTC,
			want: "2023-02-15 04:02:12.321 +00:00",
		},
		{
			in:   "2023-02-15 04:02:12.321 +00:00",
			loc:  time.UTC,
			want: "2023-02-15 04:02:12.321 +00:00",
		},
	}
	for _, tt := range tests {
		in, _ := time.Parse(defaultDateTime64FormatWithZone, tt.in)
		got := getTimeWithDifferentLocation(in, tt.loc)
		assert.Equal(t, tt.want, got.Format(defaultDateTime64FormatWithZone))
	}
}

var benchmarkResultTime time.Time

func BenchmarkGetTimeWithDifferentLocation(b *testing.B) {
	t := time.Date(2023, time.April, 12, 1, 12, 33, 0, time.UTC)
	loc := time.FixedZone("", 4*60*60)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkResultTime = getTimeWithDifferentLocation(t, loc)
	}
}
