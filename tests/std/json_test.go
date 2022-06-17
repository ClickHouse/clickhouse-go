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

package std

import (
	"encoding/json"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type Releases struct {
	Version string
}

type Repository struct {
	URL      string `json:"url"`
	Releases []Releases
}

type Achievement struct {
	Name        string
	AwardedDate time.Time
}
type Account struct {
	Id            uint32
	Name          string
	Organizations []string `json:"orgs"`
	Repositories  []Repository
	Achievement   Achievement
}

type GithubEvent struct {
	Title        string
	Type         string
	Assignee     Account  `json:"assignee"`
	Labels       []string `json:"labels"`
	Contributors []Account
	// should not be exported
	createdAt string
}

var testDate, _ = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", "2022-05-25 17:20:57 +0100 WEST")

func toJson(obj interface{}) string {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "unable to marshal"
	}
	return string(bytes)
}

func TestStdJson(t *testing.T) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Settings: clickhouse.Settings{
			"allow_experimental_object_type": 1,
		},
	})
	if err := checkMinServerVersion(conn, 22, 6, 1); err != nil {
		t.Skip(err.Error())
		return
	}
	conn.Exec("DROP TABLE json_test")
	const ddl = `
		CREATE TABLE json_test (
			  event JSON
		) Engine Memory
		`
	defer func() {
		conn.Exec("DROP TABLE json_test")
	}()
	_, err := conn.Exec(ddl)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	batch, err := scope.Prepare("INSERT INTO json_test")
	require.NoError(t, err)
	col1Data := GithubEvent{
		Title: "Document JSON support",
		Type:  "Issue",
		Assignee: Account{
			Id:            1244,
			Name:          "Geoff",
			Achievement:   Achievement{Name: "Mars Star", AwardedDate: testDate.Truncate(time.Second)},
			Repositories:  []Repository{{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}, {URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}},
			Organizations: []string{"Support Engineer", "Integrations"},
		},
		Labels: []string{"Help wanted"},
		Contributors: []Account{
			{Id: 2244, Achievement: Achievement{Name: "Adding JSON to go driver", AwardedDate: testDate.Truncate(time.Second).Add(time.Hour * -500)}, Organizations: []string{"Support Engineer", "Consulting", "PM", "Integrations"}, Name: "Dale", Repositories: []Repository{{URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}, {URL: "https://github.com/grafana/clickhouse", Releases: []Releases{{Version: "1.2.0"}, {Version: "1.3.0"}}}}},
			{Id: 2344, Achievement: Achievement{Name: "Managing S3 buckets", AwardedDate: testDate.Truncate(time.Second).Add(time.Hour * -700)}, Organizations: []string{"Support Engineer", "Consulting"}, Name: "Melyvn", Repositories: []Repository{{URL: "https://github.com/ClickHouse/support", Releases: []Releases{{Version: "1.0.0"}, {Version: "2.3.0"}, {Version: "2.4.0"}}}}},
		},
	}
	_, err = batch.Exec(col1Data)
	require.NoError(t, scope.Commit())
	require.NoError(t, err)
	// std. interface requires we read with slices as JSON is a tuple. Avoid and use native which is more natural.
	var event []interface{}
	err = conn.QueryRow("SELECT * FROM json_test").Scan(&event)
	require.NoError(t, err)
	require.JSONEq(t, `[[[["2022-05-04 21:20:57 +0100 WEST","Adding JSON to go driver"],2244,"Dale",[[[["2.0.0"],["2.1.0"]],"https://github.com/ClickHouse/clickhouse-go"],[[["1.2.0"],["1.3.0"]],"https://github.com/grafana/clickhouse"]],["Support Engineer","Consulting","PM","Integrations"]],[["2022-04-26 13:20:57 +0100 WEST","Managing S3 buckets"],2344,"Melyvn",[[[["1.0.0"],["2.3.0"],["2.4.0"]],"https://github.com/ClickHouse/support"]],["Support Engineer","Consulting"]]],"Document JSON support","Issue",[["2022-05-25 17:20:57 +0100 WEST","Mars Star"],1244,"Geoff",[[[["1.0.0"],["1.1.0"]],"https://github.com/ClickHouse/clickhouse-python"],[[["2.0.0"],["2.1.0"]],"https://github.com/ClickHouse/clickhouse-go"]],["Support Engineer","Integrations"]],["Help wanted"]]`, toJson(event))
}
