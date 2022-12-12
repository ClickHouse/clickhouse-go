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

package resources

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var m Meta = Meta{
	ClickhouseVersions: []proto.Version{
		{
			Major: 21,
			Minor: 3,
			Patch: 0,
		},
		{
			Major: 21,
			Minor: 8,
			Patch: 0,
		},
		{
			Major: 22,
			Minor: 5,
			Patch: 5,
		},
		{
			Major: 22,
			Minor: 6,
			Patch: 6,
		},
		{
			Major: 22,
			Minor: 7,
			Patch: 8,
		},
	},
}

func TestFindGreatestVersion(t *testing.T) {
	assert.Equal(t, proto.Version{
		Major: 22,
		Minor: 7,
		Patch: 8,
	}, m.findGreatestVersion())
}

func TestSupportedVersions(t *testing.T) {
	assert.Equal(t, "21.3.0, 21.8.0, 22.5.5, 22.6.6, 22.7.8", m.SupportedVersions())
}

func TestIsSupportedClickHouseVersion(t *testing.T) {
	m.hVersion = m.findGreatestVersion()
	require.True(t, m.IsSupportedClickHouseVersion(proto.Version{
		Major: 22,
		Minor: 5,
		Patch: 6,
	}))
	require.True(t, m.IsSupportedClickHouseVersion(proto.Version{
		Major: 22,
		Minor: 5,
		Patch: 8,
	}))
	require.True(t, m.IsSupportedClickHouseVersion(proto.Version{
		Major: 22,
		Minor: 6,
		Patch: 7,
	}))
	require.True(t, m.IsSupportedClickHouseVersion(proto.Version{
		Major: 21,
		Minor: 3,
		Patch: 0,
	}))

	require.False(t, m.IsSupportedClickHouseVersion(proto.Version{
		Major: 22,
		Minor: 6,
		Patch: 1,
	}))

	require.False(t, m.IsSupportedClickHouseVersion(proto.Version{
		Major: 22,
		Minor: 4,
		Patch: 1,
	}))
}
