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

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func setupConnection(t *testing.T) driver.Conn {
	SkipOnCloud(t, "The JSON data type is an obsolete feature on Cloud.")

	conn, err := GetNativeConnection(clickhouse.Settings{
		"allow_experimental_object_type": 1,
	}, nil, nil)
	require.NoError(t, err)
	return conn
}

func setupTest(t *testing.T) (driver.Conn, func(t *testing.T)) {
	ctx := context.Background()
	conn := setupConnection(t)
	if !CheckMinServerServerVersion(conn, 22, 6, 1) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
	}
	conn.Exec(ctx, "DROP TABLE IF EXISTS json_test")
	ddl := `CREATE table json_test(event JSON) ENGINE=MergeTree() ORDER BY tuple();`
	require.NoError(t, conn.Exec(ctx, ddl))
	return conn, func(t *testing.T) {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS json_test"))
	}
}

func prepareBatch(t *testing.T, conn driver.Conn, ctx context.Context) driver.Batch {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO json_test")
	require.NoError(t, err)
	return batch
}

func toJson(obj any) string {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "unable to marshal"
	}
	return string(bytes)
}

func TestSimpleJSON(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := Repository{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	var (
		event Repository
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(row1), toJson(event))
}

func TestComplexJSON(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := GithubEvent{
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
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	var (
		event GithubEvent
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(row1), toJson(event))
}

// note decimal currently can't distinguish between null and 0 due to underlying lib - see https://github.com/shopspring/decimal/issues/219
// it also serializes decimals as strings (as Decimal is supported in JSON type), thus No guarantee that exp and value will be same with deserialized back to Decimal
func TestJSONDecimal(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	type Release struct {
		Version           decimal.Decimal
		SupportedVersions []decimal.Decimal
	}
	batch := prepareBatch(t, conn, ctx)
	row1 := Release{Version: decimal.RequireFromString("33.22"), SupportedVersions: []decimal.Decimal{decimal.RequireFromString("2.22"), decimal.RequireFromString("4.22")}}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	var (
		event Release
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	require.Equal(t, row1, event)

}

func TestJSONIP(t *testing.T) {
	type Login struct {
		Username string `json:"username"`
		IP       net.IP `json:"ip_address"`
		IPs      []net.IP
		Row      uint8
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := Login{Username: "Gingerwizard", IP: net.ParseIP("85.242.48.167"), IPs: []net.IP{net.ParseIP("22.242.48.167"), net.ParseIP("24.242.48.167")}, Row: 0}
	row2 := Login{Username: "genzgd", Row: 1, IPs: []net.IP{}}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Append(row2))
	require.NoError(t, batch.Send())
	var (
		event Login
	)
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.Row ASC")
	defer rows.Close()
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		require.NoError(t, rows.Scan(&event))
		if i == 0 {
			assert.JSONEq(t, toJson(row1), toJson(event))
		} else {
			assert.JSONEq(t, toJson(row2), toJson(event))
		}
		i++
	}
}

func TestJSONUUID(t *testing.T) {
	type Login struct {
		Username string    `json:"username"`
		UUID     uuid.UUID `json:"uuid"`
		UUIDs    []uuid.UUID
		Row      uint8
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := Login{Username: "gingerwizard", UUID: uuid.New(), Row: 0, UUIDs: []uuid.UUID{uuid.New(), uuid.New()}}
	row2 := Login{Username: "genzgd", Row: 1, UUIDs: []uuid.UUID{}}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Append(row2))
	require.NoError(t, batch.Send())
	event := make(map[string]any)
	i := 0
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.Row ASC")
	defer rows.Close()
	require.NoError(t, err)
	for rows.Next() {
		assert.NoError(t, rows.Scan(&event))
		if i == 0 {
			assert.JSONEq(t, toJson(row1), toJson(event))
		} else {
			assert.JSONEq(t, toJson(row2), toJson(event))
		}
		i++
	}

}

func TestMultipleJSONRows(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := GithubEvent{
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
	row2 := GithubEvent{
		Title: "JSON support",
		Type:  "Pull Request",
		Assignee: Account{
			Id:            2244,
			Name:          "Dale",
			Achievement:   Achievement{Name: "Arctic Vault", AwardedDate: testDate.Truncate(time.Second).Add(time.Hour * -1000)},
			Repositories:  []Repository{{URL: "https://github.com/grafana/clickhouse", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.4.0"}, {Version: "1.6.0"}}}, {URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}},
			Organizations: []string{"Support Engineer", "Integrations"},
		},
		Labels: []string{"Bug"},
		Contributors: []Account{
			{Id: 1244, Name: "Geoff", Achievement: Achievement{Name: "Mars Star", AwardedDate: testDate.Truncate(time.Second).Add(time.Hour * -3000)}, Repositories: []Repository{{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}, {URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}}, Organizations: []string{"Support Engineer", "Integrations"}},
			{Id: 2244, Achievement: Achievement{Name: "Managing S3 buckets", AwardedDate: testDate.Truncate(time.Second).Add(time.Hour * -500)}, Organizations: []string{"ClickHouse", "Consulting"}, Name: "Melyvn", Repositories: []Repository{{URL: "https://github.com/ClickHouse/support", Releases: []Releases{{Version: "1.0.0"}, {Version: "2.3.0"}, {Version: "2.3.0"}}}}},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Append(row2))
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.assignee.Id")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var (
		event GithubEvent
	)
	i := 0
	for rows.Next() {
		require.NoError(t, rows.Scan(&event))
		if i == 0 {
			assert.JSONEq(t, toJson(row1), toJson(event))
		} else {
			assert.JSONEq(t, toJson(row2), toJson(event))
		}
		i++
	}
}

func TestJSONStructWithInterface(t *testing.T) {
	type Login struct {
		Username any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	login := Login{Username: "gingerwizard"}
	require.NoError(t, batch.Append(login))
	require.NoError(t, batch.Send())
	event := make(map[string]string)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(login), toJson(event))
}

func TestJSONStructWithStructInterface(t *testing.T) {
	type Login struct {
		Username any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	nLogin := Login{Username: Login{Username: "gingerwizard"}}
	require.NoError(t, batch.Append(nLogin))
	require.NoError(t, batch.Send())
	event := make(map[string]Login)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(nLogin), toJson(event))
}

func TestJSONSlicedInterfaceInconsistent(t *testing.T) {
	type Login struct {
		Random []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	event := Login{Random: []any{"gingerwizard", int64(2222341)}}
	// Inconsistent slices not supported
	require.Error(t, batch.Append(event))
}

func TestJSONSlicedInterface(t *testing.T) {
	type Login struct {
		Random []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	login := Login{Random: []any{"gingerwizard", "geoff"}}
	require.NoError(t, batch.Append(login))
	require.NoError(t, batch.Send())
	var event Login
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(&event), toJson(login))
}

func TestJSONSlicedNilInterfaceStart(t *testing.T) {
	type Login struct {
		Random []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	login := Login{Random: []any{nil, "gingerwizard", nil, "geoff"}}
	require.NoError(t, batch.Append(login))
	require.NoError(t, batch.Send())
	var event Login
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, `{"Random":["", "gingerwizard", "", "geoff"]}`, toJson(event))
}

func TestJSONSlicedAllNils(t *testing.T) {
	type Login struct {
		Random  []any
		SomeStr string
	}
	conn, _ := setupTest(t)
	// defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	login := Login{Random: []any{nil, nil, nil}, SomeStr: "Astring"}
	require.NoError(t, batch.Append(login))
	require.NoError(t, batch.Send())
	var event Login
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, `{"Random": null, "SomeStr": "Astring"}`, toJson(event))
}

func TestJSONSlicedInterfaceFloat(t *testing.T) {
	type Login struct {
		Random []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	login := Login{Random: []any{1.1, 1.4}}
	require.NoError(t, batch.Append(login))
	require.NoError(t, batch.Send())
	var event Login
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(&event), toJson(login))
}

func TestJSONSlicedInterfaceInt(t *testing.T) {
	type Login struct {
		Random []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	//need int64
	login := Login{Random: []any{int64(1), int64(2)}}
	require.NoError(t, batch.Append(login))
	require.NoError(t, batch.Send())
	var event Login
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(&event), toJson(login))
}

func TestJSONSlicedInterfaceMixed(t *testing.T) {
	type Login struct {
		Random []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	event := Login{Random: []any{1, 2.3}}
	// This will error - currently not permitted as float can't be converted to int - first value determines type
	require.Error(t, batch.Append(event))
}

func TestJSONSlicedInterfaceMixedConvertable(t *testing.T) {
	type Login struct {
		Random []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	// this will not work - numbers currently not coerced to strings
	event := Login{Random: []any{"2.4", 2, 5.6}}
	require.Error(t, batch.Append(event))
}

func TestJSONSlicedInterfaceMap(t *testing.T) {
	type Login struct {
		Values []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	event := Login{
		Values: []any{
			map[string][]any{
				"Random": {2.1, 2, 5.6},
			},
			map[string][]any{
				"Random": {2, 2},
			},
		},
	}
	require.Error(t, batch.Append(event))
}

func TestJSONSlicedInterfaceInconsistentMap(t *testing.T) {
	type Login struct {
		Values []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	event := Login{
		Values: []any{
			map[string][]any{
				"Random": {"2.4", 2, 5.6},
			},
			map[string][]int64{
				"Random": {3, 1},
			},
		},
	}
	require.Error(t, batch.Append(event))
}

func TestJSONSlicedInterfaceConsistentMapStruct(t *testing.T) {
	type Login struct {
		Values []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	logins := Login{
		Values: []any{
			Login{
				Values: []any{
					"2.4", "2", "5.6",
				},
			},
			map[string][]any{
				"Values": {"3", "1"},
			},
		},
	}
	// This will error - can't mix objects
	require.NoError(t, batch.Append(logins))
	require.NoError(t, batch.Send())
	event := make(map[string]any)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(event), toJson(logins))
}

func TestJSONSlicedInterfaceInConsistentMapStruct(t *testing.T) {
	type Login struct {
		Values []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	event := Login{
		Values: []any{
			Login{
				Values: []any{
					2.4, 2, 5.6,
				},
			},
			map[string][]any{
				"Values": {3, int8(1)},
			},
		},
	}
	require.Error(t, batch.Append(event))
}

func TestJSONSlicedInterfaceCompatibleObjects(t *testing.T) {
	type Login struct {
		Values []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	logins := Login{
		Values: []any{
			Login{
				Values: []any{
					2.4, 2.1, 5.6,
				},
			},
			map[string][]any{
				"Random": {int64(3), int64(65)},
			},
		},
	}
	// types dont differ in dimensions
	require.NoError(t, batch.Append(logins))
	require.NoError(t, batch.Send())
	event := make(map[string]any)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, `{"Values":[{"Random":[],"Values":[2.4,2.1,5.6]},{"Random":[3,65],"Values":[]}]}`, toJson(event))
}

func TestJSONSlicedInterfaceInConsistentObjects(t *testing.T) {
	type Login struct {
		Values []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	logins := Login{
		Values: []any{
			Login{
				Values: []any{
					2.4, 2.1, 5.6,
				},
			},
			map[string][]any{
				"Values": {
					map[string]any{
						"c": "d",
					},
				},
			},
		},
	}
	require.Error(t, batch.Append(logins))
}

func TestJSONSlicedInterfaceInConsistentTypes(t *testing.T) {
	type Login struct {
		Values []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	logins := Login{
		Values: []any{
			Login{
				Values: []any{
					2.4, map[string]any{
						"random": "will fail",
					},
				},
			},
		},
	}
	require.Error(t, batch.Append(logins))
}

func TestJSONSlicedInterfaceStruct(t *testing.T) {
	type Login struct {
		Random []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	event := Login{
		Random: []any{
			Login{
				Random: []any{
					"2.4", 2, 5.6,
				},
			}, Login{
				Random: []any{
					"2.4", 1,
				},
			},
		},
	}
	require.Error(t, batch.Append(event))
}

func TestJSONSlicedInterfaceSlice(t *testing.T) {
	type Login struct {
		Random []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	logins := Login{
		Random: []any{
			[]any{"dale", "geoff"},
			[]any{"mike", "alexy"},
		},
	}
	require.NoError(t, batch.Append(logins))
	require.NoError(t, batch.Send())
	event := make(map[string]any)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(event), toJson(logins))
}

func TestJSONString(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	for i := 0; i < 100; i++ {
		assert.NoError(t, batch.Append(fmt.Sprintf("{\"url\":\"https://github.com/ClickHouse/clickhouse-python\",\"Releases\":[{\"Version\":\"1.0.0\"},{\"Version\":\"1.1.0\"}],\"Row\":%d}", i)))
	}
	require.NoError(t, batch.Send())
	type Repository struct {
		URL      string `json:"url"`
		Releases []Releases
		Row      uint8
	}
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.Row ASC")
	require.NoError(t, err)
	defer rows.Close()
	var (
		event Repository
	)
	i := 0
	for rows.Next() {
		assert.NoError(t, rows.Scan(&event))
		expectedRow := Repository{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}, Row: uint8(i)}
		assert.Equal(t, expectedRow, event)
		i++
	}
}

func TestJSONTypedMapInsert(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	type Login struct {
		IP      net.IP `json:"ip_address"`
		Row     uint8
		Details []map[string]uint64
	}
	logins := make(map[string]map[string]Login)
	logins["monday"] = make(map[string]Login)
	logins["monday"]["gingerwizard"] = Login{
		IP:  net.ParseIP("85.242.48.167"),
		Row: 0,
		Details: []map[string]uint64{
			{
				"src_port":  uint64(232323),
				"dest_port": uint64(9000),
			},
			{
				"src_port":  uint64(132323),
				"dest_port": uint64(8000),
			},
		},
	}
	logins["tuesday"] = make(map[string]Login)
	logins["tuesday"]["geogenz"] = Login{
		IP:  net.ParseIP("22.242.48.167"),
		Row: 1,
		Details: []map[string]uint64{
			{
				"src_port":  uint64(34234),
				"dest_port": uint64(8000),
			},
			{
				"src_port":  uint64(932323),
				"dest_port": uint64(9200),
			},
		},
	}
	require.NoError(t, batch.Append(logins))
	require.NoError(t, batch.Send())
	event := make(map[string]map[string]Login)
	assert.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(event), toJson(logins))
}

func TestJSONUnTypedMapInsert(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	type Login struct {
		IP      net.IP `json:"ip_address"`
		Row     uint8
		Details map[string]any
	}
	logins := make(map[string]any)
	logins["monday"] = Login{
		IP:  net.ParseIP("85.242.48.167"),
		Row: 0,
		Details: map[string]any{
			"src_port":  int64(232323),
			"dest_port": int64(9000),
		},
	}
	require.NoError(t, batch.Append(logins))
	require.NoError(t, batch.Send())
	event := make(map[string]Login)
	assert.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(logins), toJson(event))
}

func TestJSONMapInconsistentMap(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	type Login struct {
		IP      net.IP `json:"ip_address"`
		Row     uint8
		Details []map[string]uint64
	}
	logins := make(map[string]map[string]Login)
	logins["session"] = make(map[string]Login)
	logins["session"]["user"] = Login{
		IP:  net.ParseIP("85.242.48.167"),
		Row: 0,
		Details: []map[string]uint64{
			{
				"src_port":  uint64(232323),
				"dest_port": uint64(9000),
			},
			{
				"dest_port":  uint64(8000),
				"proxy_port": uint64(8080),
			},
			{
				"server_port": uint64(8000),
				"local_port":  uint64(232323),
			},
		},
	}

	type Port struct {
		SourcePort uint64 `json:"src_port"`
		DestPort   uint64 `json:"dest_port"`
		ProxyPort  uint64 `json:"proxy_port"`
		ServerPort uint64 `json:"server_port"`
		LocalPort  uint64 `json:"local_port"`
	}

	type DefaultLogin struct {
		IP      net.IP `json:"ip_address"`
		Row     uint8
		Details []Port
	}

	type User struct {
		User DefaultLogin `json:"user"`
	}
	type Logins struct {
		Session User `json:"session"`
	}
	require.NoError(t, batch.Append(logins))
	require.NoError(t, batch.Send())
	event := Logins{Session: User{User: DefaultLogin{
		Row: 1,
	}}}
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, `{
							  "session": {
								"user": {
								  "ip_address": "85.242.48.167",
								  "Row": 0,
								  "Details": [
									{
									  "src_port": 232323,
									  "dest_port": 9000,
									  "proxy_port": 0,
									  "server_port": 0,
									  "local_port": 0
									},
									{
									  "src_port": 0,
									  "dest_port": 8000,
									  "proxy_port": 8080,
									  "server_port": 0,
									  "local_port": 0
									},
									{
									  "src_port": 0,
									  "dest_port": 0,
									  "proxy_port": 0,
									  "server_port": 8000,
									  "local_port": 232323
									}
								  ]
								}
							  }
							}
							`, toJson(event))
}

func TestJSONMapInconsistentMapWithInterface(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	type Login struct {
		IP      net.IP `json:"ip_address"`
		Row     uint8
		Details []map[string]any
	}
	logins := make(map[string]map[string]any)
	logins["session"] = make(map[string]any)
	logins["session"]["user"] = []Login{{
		IP:  net.ParseIP("85.242.48.167"),
		Row: 0,
		Details: []map[string]any{
			{
				"src_port":  uint64(232323),
				"dest_port": uint64(9000),
			},
			{
				"dest_port":  uint64(8000),
				"proxy_port": uint64(8080),
			},
			{
				"server_port": uint64(8000),
				"local_port":  uint64(232323),
			},
		},
	}}
	require.NoError(t, batch.Append(logins))
	require.NoError(t, batch.Send())
	event := make(map[string]map[string][]Login)
	if err := conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event); assert.NoError(t, err) {
		assert.JSONEq(t, `{
							  "session": {
								"user": [{
								  "Details": [
									{
									  "dest_port": 9000,
									  "local_port": 0,
									  "proxy_port": 0,
									  "server_port": 0,
									  "src_port": 232323
									},
									{
									  "dest_port": 8000,
									  "local_port": 0,
									  "proxy_port": 8080,
									  "server_port": 0,
									  "src_port": 0
									},
									{
									  "dest_port": 0,
									  "local_port": 232323,
									  "proxy_port": 0,
									  "server_port": 8000,
									  "src_port": 0
									}
								  ],
								  "Row": 0,
								  "ip_address": "85.242.48.167"
								}]
							  }
							}`, toJson(event))
	}
}

func TestJSONMapInconsistentComplexMap(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	type Login struct {
		IP      string
		Row     uint8
		Details []map[string]any
	}
	batch := prepareBatch(t, conn, ctx)
	logins := map[string][]map[string]any{
		"server": {
			{
				"a": "b",
				"l": []string{"z", "x"},
				"y": [][]map[string]any{
					{
						{
							"a": "b",
							"c": "d",
						},
						{
							"d": "e",
							"f": "g",
						},
						{
							"a": "b",
							"z": "e",
						},
					},
					{
						{
							"a": "b",
							"c": "d",
						},
						{
							"k": "l",
						},
					},
				},
			},
			{
				"b": []string{"c", "d"},
				"c": []map[string]any{
					{
						"g": Login{
							IP:  "11.242.48.167",
							Row: 4,
							Details: []map[string]any{
								{
									"src_port":  uint64(132323),
									"dest_port": uint64(20000),
								},
								{
									"dest_port":  uint64(8000),
									"proxy_port": uint64(4080),
									"local_port": uint64(132323),
								},
							},
						},
					},
				},
				"p": [][]map[string]any{
					{
						{
							"c": "d",
						},
						{
							"d": "e",
							"l": "g",
						},
						{
							"a": "b",
						},
					},
					{
						{
							"a": "b",
						},
						{
							"k": "l",
						},
					},
				},
			},
		},
	}
	require.NoError(t, batch.Append(logins))
	require.NoError(t, batch.Send())
	// TODO: current issue with slices and slices at insert time
	/*event := make(map[string]any)
	conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(event)
	*/
}

func TestJSONNestedStruct(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	type Port struct {
		SrcPort    uint64
		DestPort   uint64
		ProxyPort  uint64
		ServerPort uint64
		LocalPort  uint64
	}

	type l2 struct {
		A string
		C []Port
	}

	type l1 struct {
		Server []l2
	}

	logins := l1{Server: []l2{
		{
			A: "b",
		},
		{
			C: []Port{
				{
					SrcPort:  uint64(232323),
					DestPort: uint64(9000),
				},
				{
					ProxyPort: uint64(8080),
					DestPort:  uint64(8000),
				},
				{
					ServerPort: uint64(8000),
					LocalPort:  uint64(232323),
				},
			},
		},
	}}
	require.NoError(t, batch.Append(logins))
	require.NoError(t, batch.Send())
	var (
		row1 l1
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&row1))
	assert.JSONEq(t, `{
							  "Server": [
								{
								  "A": "b",
								  "C": []
								},
								{
								  "A": "",
								  "C": [
									{
									  "SrcPort": 232323,
									  "DestPort": 9000,
									  "ProxyPort": 0,
									  "ServerPort": 0,
									  "LocalPort": 0
									},
									{
									  "SrcPort": 0,
									  "DestPort": 8000,
									  "ProxyPort": 8080,
									  "ServerPort": 0,
									  "LocalPort": 0
									},
									{
									  "SrcPort": 0,
									  "DestPort": 0,
									  "ProxyPort": 0,
									  "ServerPort": 8000,
									  "LocalPort": 232323
									}
								  ]
								}
							  ]
							}
							`, toJson(row1))
}

func TestJSONMapSimpleInconsistentRows(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	type Login struct {
		IP  string
		Row uint8
	}
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string]map[string]any{
		"i": {
			"row": int64(0),
		},
		"a": {
			"d": "test",
		},
		"c": {
			"e": "test",
		},
		"k": {
			"h": Login{
				IP:  "127.0.0.1",
				Row: 0,
			},
		},
	}
	row2 := map[string]map[string]any{
		"i": {
			"row": int64(1),
		},
		"a": {
			"d": "test",
		},
		"d": {
			"e": "test",
		},
		"g": {
			"h": Login{
				IP:  "127.0.0.1",
				Row: 0,
			},
		},
		"z": {
			"c": []map[string]Login{
				{
					"f": Login{
						IP:  "127.2.0.1",
						Row: 0,
					},
					"g": Login{
						IP:  "127.3.0.1",
						Row: 1,
					},
				},
			},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Append(row2))
	require.NoError(t, batch.Send())
	event := make(map[string]map[string]any)
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.i.row ASC")
	defer rows.Close()
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		require.NoError(t, rows.Scan(&event))
		if i == 0 {
			// clickhouse fills in emptys
			assert.JSONEq(t, `{"a":{"d":"test"},"c":{"e":"test"},"d":{"e":""},"g":{"h":{"IP":"","Row":0}},"i":{"row":0},"k":{"h":{"IP":"127.0.0.1","Row":0}},"z":{"c":[]}}`, toJson(event))
		} else {
			assert.JSONEq(t, `{"a":{"d":"test"},"c":{"e":""},"d":{"e":"test"},"g":{"h":{"IP":"127.0.0.1","Row":0}},"i":{"row":1},"k":{"h":{"IP":"","Row":0}},"z":{"c":[{"f":{"IP":"127.2.0.1","Row":0},"g":{"IP":"127.3.0.1","Row":1}}]}}`, toJson(event))
		}
		i++
	}
}

func TestJSONMapInconsistentRowsOfSlices(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string][]map[string]any{
		"i": {
			{
				"row": int64(0),
			},
		},
		"a": {
			{
				"d": []string{"z", "f"},
				"f": []string{"x", "f"},
			},
			{
				"d": []string{"e", "f"},
				"g": []string{"x", "f"},
			},
		},
	}
	row2 := map[string][]map[string]any{
		"i": {
			{
				"row": int64(1),
			},
		},
		"b": {
			{
				"a": []string{"c", "d"},
			},
			{
				"d": []string{"e", "f"},
			},
		},
		"a": {
			{
				"f": []string{"z", "f"},
				"z": []string{"x", "f"},
			},
			{
				"a": []string{"e", "f"},
				"n": []string{"x", "f"},
			},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Append(row2))
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.i.row ASC")
	defer rows.Close()
	require.NoError(t, err)
	i := 0
	event := make(map[string][]map[string]any)
	for rows.Next() {
		require.NoError(t, rows.Scan(&event))
		if i == 0 {
			// clickhouse fills in empty values
			assert.JSONEq(t, `{"a":[{"a":[],"d":["z","f"],"f":["x","f"],"g":[],"n":[],"z":[]},{"a":[],"d":["e","f"],"f":[],"g":["x","f"],"n":[],"z":[]}],"b":[],"i":[{"row":0}]}`, toJson(event))
		} else {
			// clickhouse fills in empty values
			assert.JSONEq(t, `{"a":[{"a":[],"d":[],"f":["z","f"],"g":[],"n":[],"z":["x","f"]},{"a":["e","f"],"d":[],"f":[],"g":[],"n":["x","f"],"z":[]}],"b":[{"a":["c","d"],"d":[]},{"a":[],"d":["e","f"]}],"i":[{"row":1}]}`, toJson(event))
		}
		i++
	}
}

func TestJSONInconsistentStruct(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	type l3 struct {
		D string
	}

	type l2 struct {
		E []l3
		G []string
		M any
	}

	type l1 struct {
		I int64
		C l2
		A l3
		E l3
		F any
	}

	s1 := l1{
		C: l2{
			E: []l3{{D: "test"}},
			G: []string{"test1", "test2"},
			M: "test",
		},
		A: l3{
			D: "test",
		},
	}

	s2 := l1{
		I: 1,
		A: l3{
			D: "test",
		},
		E: l3{
			D: "test",
		},
		F: l3{
			D: "interface_test",
		},
	}

	require.NoError(t, batch.Append(s1))
	require.NoError(t, batch.Append(s2))
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.I ASC")
	defer rows.Close()
	require.NoError(t, err)
	i := 0
	var event l1
	for rows.Next() {
		require.NoError(t, rows.Scan(&event))
		if i == 0 {
			// clickhouse fills in empty values
			assert.JSONEq(t, `{"I":0,"C":{"E":[{"D":"test"}],"G":["test1","test2"],"M":"test"},"A":{"D":"test"},"E":{"D":""},"F":{"D":""}}`, toJson(event))
		} else {
			// clickhouse fills in empty values
			assert.JSONEq(t, `{"I":1,"C":{"E":[],"G":[],"M":""},"A":{"D":"test"},"E":{"D":"test"},"F":{"D":"interface_test"}}`, toJson(event))
		}
		i++
	}
}

func TestJSONMapInconsistentRows(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)

	row1 := map[string]map[string]any{
		"i": {
			"row": int8(0),
		},
		"a": {
			"d": "test",
		},
		"c": {
			"e": []map[string]float64{{
				"f": float64(122.1),
			}},
		},
		"f": {
			"k": [][]string{
				{
					"a", "b", "c",
				},
				{
					"d", "e", "f",
				},
			},
		},
		"z": {
			"uuid": uuid.MustParse("71224334-0422-4864-830f-87b2bf2eedb0"),
		},
		"x": {
			"uuids": []uuid.UUID{uuid.MustParse("61224334-0422-4864-830f-87b2bf2eedb0"), uuid.MustParse("21988fd4-0620-41ea-9202-729343ba9e88")},
		},
	}

	row2 := map[string]map[string]any{
		"i": {
			"row": int8(1),
		},
		"a": {
			"d": "test",
		},
		"l": {
			"m": [][]string{
				{
					"d", "e", "f",
				},
			},
		},
		"d": {
			"now": testDate,
		},
		"t": {
			"uuid": uuid.MustParse("00d6d7e3-f960-42d4-a4db-f95513762841"),
		},
		"n": {
			"dates": []time.Time{testDate, testDate},
		},
	}

	row3 := map[string]map[string]any{
		"i": {
			"row": int8(2),
		},
	}

	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Append(row2))
	require.NoError(t, batch.Append(row3))
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.i.row ASC")
	defer rows.Close()
	require.NoError(t, err)
	i := 0
	event := make(map[string]map[string]any)
	for rows.Next() {
		require.NoError(t, rows.Scan(&event))
		switch i {
		case 0:
			assert.JSONEq(t, `{"a":{"d":"test"},"c":{"e":[{"f":122.1}]},"d":{"now":"0001-01-01 00:00:00 +0000 UTC"},"f":{"k":[["a","b","c"],["d","e","f"]]},"i":{"row":0},"l":{"m":[]},"n":{"dates":[]},"t":{"uuid":"00000000-0000-0000-0000-000000000000"},"x":{"uuids":["61224334-0422-4864-830f-87b2bf2eedb0","21988fd4-0620-41ea-9202-729343ba9e88"]},"z":{"uuid":"71224334-0422-4864-830f-87b2bf2eedb0"}}`, toJson(event))
		case 1:
			assert.JSONEq(t, `{"a":{"d":"test"},"c":{"e":[]},"d":{"now":"2022-05-25 17:20:57 +0100 WEST"},"f":{"k":[]},"i":{"row":1},"l":{"m":[["d","e","f"]]},"n":{"dates":["2022-05-25 17:20:57 +0100 WEST","2022-05-25 17:20:57 +0100 WEST"]},"t":{"uuid":"00d6d7e3-f960-42d4-a4db-f95513762841"},"x":{"uuids":[]},"z":{"uuid":"00000000-0000-0000-0000-000000000000"}}`, toJson(event))
		case 2:
			assert.JSONEq(t, `{"a":{"d":""},"c":{"e":[]},"d":{"now":"0001-01-01 00:00:00 +0000 UTC"},"f":{"k":[]},"i":{"row":2},"l":{"m":[]},"n":{"dates":[]},"t":{"uuid":"00000000-0000-0000-0000-000000000000"},"x":{"uuids":[]},"z":{"uuid":"00000000-0000-0000-0000-000000000000"}}`, toJson(event))
		}
		i++
	}
}

func TestJSONMapSliceOfSlices(t *testing.T) {
	//map[string][][]map[string]type
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string][][]uuid.UUID{
		"uuids": {
			[]uuid.UUID{uuid.MustParse("10d6d7e3-f960-42d4-a4db-f95513762841"), uuid.MustParse("20d6d7e3-f960-42d4-a4db-f95513762841")},
			[]uuid.UUID{uuid.MustParse("30d6d7e3-f960-42d4-a4db-f95513762841")},
			[]uuid.UUID{uuid.MustParse("a0d6d7e3-f960-42d4-a4db-f95513762841"), uuid.MustParse("c0d6d7e3-f960-42d4-a4db-f95513762841")},
			[]uuid.UUID{uuid.MustParse("b0d6d7e3-f960-42d4-a4db-f95513762841")},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	//read into a typed map
	event := make(map[string][][]uuid.UUID)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(row1), toJson(event))

	//read into a generic map
	genEvent := make(map[string]any)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&genEvent))
	assert.JSONEq(t, toJson(row1), toJson(genEvent))

	//read into a slice of any
	genSliceEvent := make(map[string][]any)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&genSliceEvent))
	assert.JSONEq(t, toJson(row1), toJson(genSliceEvent))

	///read into a struct
	type UUIDSets struct {
		Uuids [][]any `json:"uuids"`
	}
	var sEvent UUIDSets
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&sEvent))
	assert.JSONEq(t, toJson(row1), toJson(sEvent))

	//read in interface struct
	type UUIDIntSet struct {
		Uuids any `json:"uuids"`
	}
	var siEvent UUIDIntSet
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&siEvent))
	assert.JSONEq(t, toJson(row1), toJson(siEvent))
	//read in interface[] struct

	type UUIDArraySet struct {
		Uuids []any `json:"uuids"`
	}
	var arEvent UUIDArraySet
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&arEvent))
	assert.JSONEq(t, toJson(row1), toJson(arEvent))
}

func TestJSONMapSliceOfSlicesWithStruct(t *testing.T) {
	//map[string][][]struct{}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	type Login struct {
		IP  net.IP `json:"ip_address"`
		Row uint8
	}
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string][][]Login{
		"logins": {
			[]Login{{
				IP:  net.ParseIP("127.0.0.1"),
				Row: 0,
			},
				{
					IP:  net.ParseIP("85.67.1.2"),
					Row: 0,
				}},
			[]Login{{
				IP:  net.ParseIP("10.2.0.1"),
				Row: 0,
			}},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	event := make(map[string][][]Login)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(row1), toJson(event))

	mEvent := make(map[string][][]map[string]any)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&mEvent))
	assert.JSONEq(t, toJson(row1), toJson(mEvent))

}
func TestJSONMapSliceOfSlicesWithMap(t *testing.T) {
	//map[string][][]map[string]any
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string][][]map[string]any{
		"logins": {
			{
				{
					"ip":  net.ParseIP("127.0.0.1"),
					"row": int64(0),
				},
				{
					"ip":  net.ParseIP("85.67.1.2"),
					"row": int64(1),
				},
			},
			{
				{
					"ip":  net.ParseIP("10.2.0.1"),
					"row": int64(2),
				},
			},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	//read into a typed map
	event := make(map[string][][]map[string]any)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(row1), toJson(event))
	type Login struct {
		IP  net.IP `json:"ip"`
		Row uint8  `json:"row"`
	}
	sEvent := make(map[string][][]Login)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&sEvent))
	assert.JSONEq(t, toJson(row1), toJson(sEvent))
}

func TestMapSliceOfInterface(t *testing.T) {
	///query with map[string][]any on map[string][]map[string][]string
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string][]map[string][]string{
		"z": {
			{
				"a": {"b", "c", "d"},
				"e": {"f", "g", "h"},
			},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	event := make(map[string][]any)
	// currently this fails cannot set []map[string][]string into []any - maybe we can handle in future
	require.Panics(t, func() { conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event) })
}

func TestStructSliceOfInterface(t *testing.T) {
	///query with map[string][]any on map[string][]struct
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	type Ports struct {
		SourcePort      uint16
		Destinationport uint16
	}
	row1 := map[string][]Ports{
		"clickhouse": {
			{
				SourcePort:      uint16(9100),
				Destinationport: uint16(18222),
			},
			{
				SourcePort:      uint16(9200),
				Destinationport: uint16(13222),
			},
		},
		"postgresql": {
			{
				SourcePort:      uint16(5300),
				Destinationport: uint16(20001),
			},
			{
				SourcePort:      uint16(5301),
				Destinationport: uint16(20002),
			},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	event := make(map[string][]any)
	// currently this fails cannot set []map[string][]string into []any - maybe we can handle in future
	require.Panics(t, func() { conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event) })
}

func TestStructSliceInInterface(t *testing.T) {
	///test struct with interface with slice in it
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	type Junk struct {
		Data any
	}
	row1 := Junk{Data: []string{"some", "junk", "rows"}}
	batch := prepareBatch(t, conn, ctx)
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	event := make(map[string][]any)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(row1), toJson(event))
}

func TestInsertMarshaledJSON(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1s := `{"a":{"d":"test"},"c":{"e":"test"},"i":{"row":0},"k":{"h":{"IP":"127.0.0.1","Row":0}}}`
	row2s := `{"a":{"d":"test"},"d":{"e":"test"},"g":{"h":{"IP":"127.0.0.1","Row":0}},"i":{"row":1},"z":{"c":2.0}}`
	row1 := make(map[string]any)
	row2 := make(map[string]any)
	require.NoError(t, json.Unmarshal([]byte(row1s), &row1))
	require.NoError(t, json.Unmarshal([]byte(row2s), &row2))
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Append(row2))
	require.NoError(t, batch.Send())
	event := make(map[string]map[string]any)
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.i.row ASC")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		require.NoError(t, rows.Scan(&event))
		if i == 0 {
			// clickhouse fills in empty values
			assert.JSONEq(t, `{"a":{"d":"test"},"c":{"e":"test"},"d":{"e":""},"g":{"h":{"IP":"","Row":0}},"i":{"row":0},"k":{"h":{"IP":"127.0.0.1","Row":0}},"z":{"c":0}}`, toJson(event))
		} else {
			assert.JSONEq(t, `{"a":{"d":"test"},"c":{"e":""},"d":{"e":"test"},"g":{"h":{"IP":"127.0.0.1","Row":0}},"i":{"row":1},"k":{"h":{"IP":"","Row":0}},"z":{"c":2}}`, toJson(event))
		}
		i++
	}

}

func TestJSONEmbeddedStruct(t *testing.T) {
	type Person struct {
		Name string
	}
	type Child struct {
		Person
	}
	type Parent struct {
		Person
		Children []Child
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := Parent{
		Person:   Person{Name: "Dale"},
		Children: []Child{{Person{Name: "Max"}}, {Person{Name: "Xavi"}}, {Person{Name: "Zach"}}},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	event := make(map[string]any)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(row1), toJson(row1))

}

func TestJSONMissingStructFieldAtQueryTime(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := GithubEvent{
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
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	type InconsistentAccount struct {
		Id            string //inconsistent type - usually uint32
		Name          string
		Organizations []string `json:"orgs"`
		Repositories  []Repository
		// no Achievement
	}

	type InconsistentGithubEvent struct {
		Title        string
		EventType    string              // new field
		Assignee     InconsistentAccount `json:"assignee"`
		Contributors []InconsistentAccount
		// no Labels
	}
	var event InconsistentGithubEvent
	// just ignore fields in the struct we can't restore
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, `{
	  "Title": "Document JSON support",
	  "EventType": "",
	  "assignee": {
		"Id": "1244",
		"Name": "Geoff",
		"orgs": [
		  "Support Engineer",
		  "Integrations"
		],
		"Repositories": [
		  {
			"url": "https://github.com/ClickHouse/clickhouse-python",
			"Releases": [
			  {
				"Version": "1.0.0"
			  },
			  {
				"Version": "1.1.0"
			  }
			]
		  },
		  {
			"url": "https://github.com/ClickHouse/clickhouse-go",
			"Releases": [
			  {
				"Version": "2.0.0"
			  },
			  {
				"Version": "2.1.0"
			  }
			]
		  }
		]
	  },
	  "Contributors": [
		{
		  "Id": "2244",
		  "Name": "Dale",
		  "orgs": [
			"Support Engineer",
			"Consulting",
			"PM",
			"Integrations"
		  ],
		  "Repositories": [
			{
			  "url": "https://github.com/ClickHouse/clickhouse-go",
			  "Releases": [
				{
				  "Version": "2.0.0"
				},
				{
				  "Version": "2.1.0"
				}
			  ]
			},
			{
			  "url": "https://github.com/grafana/clickhouse",
			  "Releases": [
				{
				  "Version": "1.2.0"
				},
				{
				  "Version": "1.3.0"
				}
			  ]
			}
		  ]
		},
		{
		  "Id": "2344",
		  "Name": "Melyvn",
		  "orgs": [
			"Support Engineer",
			"Consulting"
		  ],
		  "Repositories": [
			{
			  "url": "https://github.com/ClickHouse/support",
			  "Releases": [
				{
				  "Version": "1.0.0"
				},
				{
				  "Version": "2.3.0"
				},
				{
				  "Version": "2.4.0"
				}
			  ]
			}
		  ]
		}
	  ]
	}
	`, toJson(event))
}

func TestJSONNilStructFields(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := GithubEvent{
		Title: "Document JSON support",
		Type:  "Issue",
		Assignee: Account{
			Id:            1233,
			Name:          "Geoff",
			Achievement:   Achievement{Name: "Mars Star", AwardedDate: testDate.Truncate(time.Second)},
			Repositories:  []Repository{{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}, {URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}},
			Organizations: []string{"Support Engineer", "Integrations"},
		},
		Labels:       nil,
		Contributors: nil,
	}
	row2 := GithubEvent{
		Title: "Document JSON issues",
		Type:  "Issue",
		Assignee: Account{
			Id:            1244,
			Name:          "Dale",
			Repositories:  nil,
			Organizations: nil,
		},
		Labels: []string{"Help wanted"},
		Contributors: []Account{
			{Id: 2244, Achievement: Achievement{Name: "Adding JSON to go driver", AwardedDate: testDate.Truncate(time.Second).Add(time.Hour * -500)}, Organizations: []string{"Support Engineer", "Consulting", "PM", "Integrations"}, Name: "Dale", Repositories: []Repository{{URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}, {URL: "https://github.com/grafana/clickhouse", Releases: []Releases{{Version: "1.2.0"}, {Version: "1.3.0"}}}}},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Append(row2))
	require.NoError(t, batch.Send())
	var event GithubEvent
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.assignee.Id ASC")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		require.NoError(t, rows.Scan(&event))
		if i == 0 {
			// clickhouse fills in empty values
			assert.JSONEq(t, `{"Title":"Document JSON support","Type":"Issue","assignee":{"Id":1233,"Name":"Geoff","orgs":["Support Engineer","Integrations"],"Repositories":[{"url":"https://github.com/ClickHouse/clickhouse-python","Releases":[{"Version":"1.0.0"},{"Version":"1.1.0"}]},{"url":"https://github.com/ClickHouse/clickhouse-go","Releases":[{"Version":"2.0.0"},{"Version":"2.1.0"}]}],"Achievement":{"Name":"Mars Star","AwardedDate":"2022-05-25T17:20:57+01:00"}},"labels":[],"Contributors":[]}`, toJson(event))
		} else {
			assert.JSONEq(t, `{"Title":"Document JSON issues","Type":"Issue","assignee":{"Id":1244,"Name":"Dale","orgs":[],"Repositories":[],"Achievement":{"Name":"","AwardedDate":"0001-01-01T00:00:00Z"}},"labels":["Help wanted"],"Contributors":[{"Id":2244,"Name":"Dale","orgs":["Support Engineer","Consulting","PM","Integrations"],"Repositories":[{"url":"https://github.com/ClickHouse/clickhouse-go","Releases":[{"Version":"2.0.0"},{"Version":"2.1.0"}]},{"url":"https://github.com/grafana/clickhouse","Releases":[{"Version":"1.2.0"},{"Version":"1.3.0"}]}],"Achievement":{"Name":"Adding JSON to go driver","AwardedDate":"2022-05-04T21:20:57+01:00"}}]}`, toJson(event))
		}
		i++
	}
}

func TestJSONNilMapFields(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string]any{
		"title": "Document JSON support",
		"type":  "Issue",
		"assignee": map[string]any{
			"id":            int16(1233),
			"name":          "Dale",
			"repositories":  nil,
			"organizations": []string{},
		},
		"labels": []string{},
		"contributors": []map[string]any{
			{"Id": int16(2244), "Name": "Dale", "orgs": []string{"Support Engineer", "Consulting", "PM", "Integrations"}, "Repositories": []map[string]any{{"url": "https://github.com/ClickHouse/clickhouse-go", "Releases": []map[string]any{{"Version": "2.0.0"}, {"Version": "2.1.0"}}}, {"url": "https://github.com/grafana/clickhouse"}}},
		},
	}
	row2 := map[string]any{
		"title": "Document JSON issues",
		"type":  "Issue",
		"assignee": map[string]any{
			"id":   int16(1244),
			"name": "Geoff",
			"repositories": []map[string]any{
				{"url": "https://github.com/ClickHouse/clickhouse-python", "Releases": []map[string]any{{"Version": "2.0.0"}, {"Version": "2.1.0"}}},
				{"url": "https://github.com/ClickHouse/clickhouse-go"},
			},
			"organizations": []string{"Support Engineer", "Integrations"},
		},
		"labels":       []string{},
		"contributors": nil,
	}

	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Append(row2))
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.assignee.id ASC")
	require.NoError(t, err)
	event := make(map[string]any)
	i := 0
	for rows.Next() {
		require.NoError(t, rows.Scan(&event))
		if i == 0 {
			// clickhouse fills in empty values and nil slices to []
			assert.JSONEq(t, `{"assignee":{"id":1233,"name":"Dale","organizations":[],"repositories":[]},"contributors":[{"Id":2244,"Name":"Dale","Repositories":[{"Releases":[{"Version":"2.0.0"},{"Version":"2.1.0"}],"url":"https://github.com/ClickHouse/clickhouse-go"},{"Releases":[],"url":"https://github.com/grafana/clickhouse"}],"orgs":["Support Engineer","Consulting","PM","Integrations"]}],"labels":[],"title":"Document JSON support","type":"Issue"}`, toJson(event))
		} else {
			assert.JSONEq(t, `{"assignee":{"id":1244,"name":"Geoff","organizations":["Support Engineer","Integrations"],"repositories":[{"Releases":[{"Version":"2.0.0"},{"Version":"2.1.0"}],"url":"https://github.com/ClickHouse/clickhouse-python"},{"Releases":[],"url":"https://github.com/ClickHouse/clickhouse-go"}]},"contributors":[],"labels":[],"title":"Document JSON issues","type":"Issue"}`, toJson(event))
		}
		i++
	}

}

func TestJSONManyColumns(t *testing.T) {
	ctx := context.Background()
	conn := setupConnection(t)
	conn.Exec(ctx, "DROP TABLE IF EXISTS json_test")
	if !CheckMinServerServerVersion(conn, 22, 6, 1) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
	}
	ddl := `CREATE table json_test(event JSON, event2 JSON, col1 String) ENGINE=MergeTree() ORDER BY tuple();`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer conn.Exec(ctx, "DROP TABLE IF EXISTS json_test")
	batch := prepareBatch(t, conn, ctx)
	col1 := Repository{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}
	col2 := Repository{URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}
	require.NoError(t, batch.Append(col1, col2, "Test"))
	require.NoError(t, batch.Send())
	var (
		event1 Repository
		event2 Repository
		sCol   string
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event1, &event2, &sCol))
	assert.JSONEq(t, toJson(col1), toJson(event1))
	assert.JSONEq(t, toJson(col2), toJson(event2))
	require.Equal(t, sCol, "Test")
}

func TestIPInInterfaceSlice(t *testing.T) {
	type Login struct {
		IPs []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	login := Login{IPs: []any{net.ParseIP("134.1.1.1"), net.ParseIP("127.0.0.1"), "127.0.0.2"}}
	require.NoError(t, batch.Append(login))
	require.NoError(t, batch.Send())
	var event Login
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(login), toJson(event))
}

func TestNilStringInInterfaceSlice(t *testing.T) {
	type Login struct {
		IPs []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	login := Login{IPs: []any{"dale", "geoff", nil}}
	require.NoError(t, batch.Append(login))
	require.NoError(t, batch.Send())
	var event Login
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, `{"IPs":["dale","geoff",""]}`, toJson(event))
}

func TestNilInNumericInterfaceSlice(t *testing.T) {
	type Login struct {
		IPs []any
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	login := Login{IPs: []any{int64(1), int64(2), nil}}
	require.NoError(t, batch.Append(login))
	require.NoError(t, batch.Send())
	var event Login
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, `{"IPs":[1,2,0]}`, toJson(event))
}

func TestJSONNonStringMap(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string]any{
		"title": "Document JSON support",
		"type":  "Issue",
		"assignee": map[int]any{
			1: "this is not permitted",
		},
	}
	require.Error(t, batch.Append(row1))
}

func TestInconsistentCompatibleTypesInBatch(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string]any{
		"title": "Document JSON support",
		"type":  "Issue",
		"assignee": map[string]any{
			"id":            int16(0),
			"name":          "Dale",
			"repositories":  nil,
			"organizations": []string{},
		},
		"labels": []string{},
		"contributors": []map[string]any{
			{"Id": int16(2244), "Name": "Dale", "orgs": []string{"Support Engineer", "Consulting", "PM", "Integrations"}, "Repositories": []map[string]any{{"url": "https://github.com/ClickHouse/clickhouse-go", "Releases": []map[string]any{{"Version": "2.0.0"}, {"Version": "2.1.0"}}}, {"url": "https://github.com/grafana/clickhouse"}}},
		},
	}
	row2 := map[string]any{
		"title": "Document JSON issues",
		"type":  "Issue",
		"assignee": map[string]any{
			"id":   int16(1),
			"name": "Geoff",
			"repositories": []Repository{
				{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}},
				{URL: "https://github.com/ClickHouse/clickhouse-go"},
			},
			"organizations": []string{"Support Engineer", "Integrations"},
		},
	}

	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Append(row2))
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM json_test ORDER BY event.assignee.id ASC")
	require.NoError(t, err)
	event := make(map[string]any)
	i := 0
	for rows.Next() {
		require.NoError(t, rows.Scan(&event))
		if i == 0 {
			// clickhouse fills in empty values and nil slices to []
			assert.JSONEq(t, `{"assignee":{"id":0,"name":"Dale","organizations":[],"repositories":[]},"contributors":[{"Id":2244,"Name":"Dale","Repositories":[{"Releases":[{"Version":"2.0.0"},{"Version":"2.1.0"}],"url":"https://github.com/ClickHouse/clickhouse-go"},{"Releases":[],"url":"https://github.com/grafana/clickhouse"}],"orgs":["Support Engineer","Consulting","PM","Integrations"]}],"labels":[],"title":"Document JSON support","type":"Issue"}`, toJson(event))
		} else {
			assert.JSONEq(t, `{"assignee":{"id":1,"name":"Geoff","organizations":["Support Engineer","Integrations"],"repositories":[{"Releases":[{"Version":"2.0.0"},{"Version":"2.1.0"}],"url":"https://github.com/ClickHouse/clickhouse-python"},{"Releases":[],"url":"https://github.com/ClickHouse/clickhouse-go"}]},"contributors":[],"labels":[],"title":"Document JSON issues","type":"Issue"}`, toJson(event))
		}
		i++
	}
}

func TestInconsistentInCompatibleTypesInBatch(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string]any{
		"title": "Document JSON support",
		"type":  "Issue",
		"assignee": map[string]any{
			"id":   int16(0),
			"name": "Dale",
			"repositories": []Repository{
				{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}},
				{URL: "https://github.com/ClickHouse/clickhouse-go"},
			},
			"organizations": []string{},
		},
		"labels": []string{},
		"contributors": []map[string]any{
			{"Id": int16(2244), "Name": "Dale", "orgs": []string{"Support Engineer", "Consulting", "PM", "Integrations"}, "Repositories": []map[string]any{{"url": "https://github.com/ClickHouse/clickhouse-go", "Releases": []map[string]any{{"Version": "2.0.0"}, {"Version": "2.1.0"}}}, {"url": "https://github.com/grafana/clickhouse"}}},
		},
	}
	row2 := map[string]any{
		"title": "Document JSON issues",
		"type":  "Issue",
		"assignee": map[string]any{
			"id":            int16(1),
			"name":          "Geoff",
			"repositories":  []string{"https://github.com/ClickHouse/clickhouse-python", "https://github.com/ClickHouse/clickhouse-go"},
			"organizations": []string{"Support Engineer", "Integrations"},
		},
	}

	require.NoError(t, batch.Append(row1))
	require.Error(t, batch.Append(row2))
}

func TestCastableTypesOnQuery(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string]any{
		"title": "Document JSON support",
		"type":  "Issue",
		"assignee": map[string]any{
			"id":           int16(0),
			"name":         "Dale",
			"orgs":         []string{"clickhouse"},
			"repositories": []map[string]any{{"url": "https://github.com/ClickHouse/clickhouse-go", "Releases": []map[string]any{{"Version": "2.0.0"}, {"Version": "2.1.0"}}}, {"url": "https://github.com/grafana/clickhouse"}},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	type Assignee struct {
		Id            uint32       `json:"id"` // we allow most types to cast to a string at query time
		Name          string       `json:"name"`
		Organizations []string     `json:"orgs"`
		Repositories  []Repository `json:"repositories"`
	}
	type Issue struct {
		Type     string   `json:"type"`
		Title    string   `json:"title"`
		Assignee Assignee `json:"assignee"`
	}

	var event Issue
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, `{"type":"Issue","title":"Document JSON support","assignee":{"id":0,"name":"Dale","orgs":["clickhouse"],"repositories":[{"url":"https://github.com/ClickHouse/clickhouse-go","Releases":[{"Version":"2.0.0"},{"Version":"2.1.0"}]},{"url":"https://github.com/grafana/clickhouse","Releases":[]}]}}`, toJson(event))
}

func TestIncompatibleTypesOnQuery(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string]any{
		"title": "Document JSON support",
		"type":  "Issue",
		"assignee": map[string]any{
			"id":           int16(0),
			"name":         "Dale",
			"orgs":         []string{"clickhouse"},
			"repositories": []map[string]any{{"url": "https://github.com/ClickHouse/clickhouse-go", "Releases": []map[string]any{{"Version": "2.0.0"}, {"Version": "2.1.0"}}}, {"url": "https://github.com/grafana/clickhouse"}},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	type Assignee struct {
		Id            uint32   `json:"id"`
		Name          string   `json:"name"`
		Organizations []string `json:"orgs"`
		Repositories  []string `json:"repositories"` // not castable into string slice
	}
	type Issue struct {
		Type     string   `json:"type"`
		Title    string   `json:"title"`
		Assignee Assignee `json:"assignee"`
	}

	var event Issue
	require.Error(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
}

func TestInconsistentStructsOnQuery(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string]any{
		"title": "Document JSON support",
		"type":  "Issue",
		"assignee": map[string]any{
			"id":           int16(0),
			"name":         "Dale",
			"orgs":         []string{"clickhouse"},
			"repositories": []map[string]any{{"url": "https://github.com/ClickHouse/clickhouse-go", "Releases": []map[string]any{{"Version": "2.0.0"}, {"Version": "2.1.0"}}}, {"url": "https://github.com/grafana/clickhouse"}},
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	type Company struct {
		City string `json:"city"`
	}
	type Issue struct {
		Type     string  `json:"type"`
		Title    string  `json:"title"`
		Assignee Company `json:"assignee"`
	}

	var event Issue
	// this won't error - types are leniant - we will populate what we can
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, `{"type":"Issue","title":"Document JSON support","assignee":{"city":""}}`, toJson(event))
}

func TestColumnFormat(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	var cols []map[string]any

	for i := 0; i < 1_000; i++ {
		cols = append(cols, map[string]any{
			"id":    int64(i),
			"title": fmt.Sprintf("doc %v", i),
		})
	}
	require.NoError(t, batch.Column(0).Append(cols))
	require.NoError(t, batch.Send())
	var count uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM json_test").Scan(&count))
	require.Equal(t, uint64(1_000), count)
}

func TestMixedBatch(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	require.NoError(t, batch.Append(map[string]any{
		"id":    int64(0),
		"title": "doc 0",
	}))
	require.Error(t, batch.Append(`{"id": 1, "title": "doc_1"}`))
}

func TestQueryMapByReference(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := Repository{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	var event map[string]any
	//if passing an uninitialized map, ensure it is passed by pointer
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(row1), toJson(event))
	// an init map can be passed by ref or by value
	event = make(map[string]any)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(row1), toJson(event))
}

func TestQueryNestedSubColumn(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	repositories := []map[string]any{{"url": "https://github.com/ClickHouse/clickhouse-go", "Releases": []map[string]any{{"Version": "2.0.0"}, {"Version": "2.1.0"}}}, {"url": "https://github.com/grafana/clickhouse"}}
	row1 := map[string]any{
		"title": "Document JSON support",
		"type":  "Issue",
		"assignee": map[string]any{
			"id":           int16(0),
			"name":         "Dale",
			"orgs":         []string{"clickhouse"},
			"repositories": repositories,
		},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	var event []map[string]any
	require.NoError(t, conn.QueryRow(ctx, "SELECT event.assignee.repositories FROM json_test").Scan(&event))
	assert.JSONEq(t, `[{"Releases":[{"Version":"2.0.0"},{"Version":"2.1.0"}],"url":"https://github.com/ClickHouse/clickhouse-go"},{"Releases":[],"url":"https://github.com/grafana/clickhouse"}]`, toJson(event))
}

func TestQueryTupleSubColumn(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	assignee := map[string]any{
		"id":           int16(0),
		"name":         "Dale",
		"orgs":         []string{"clickhouse"},
		"repositories": []map[string]any{{"url": "https://github.com/ClickHouse/clickhouse-go", "Releases": []map[string]any{{"Version": "2.0.0"}, {"Version": "2.1.0"}}}, {"url": "https://github.com/grafana/clickhouse"}},
	}
	row1 := map[string]any{
		"title":    "Document JSON support",
		"type":     "Issue",
		"assignee": assignee,
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	var event map[string]any
	require.NoError(t, conn.QueryRow(ctx, "SELECT event.assignee FROM json_test").Scan(&event))
	assert.JSONEq(t, `{"id":0,"name":"Dale","orgs":["clickhouse"],"repositories":[{"Releases":[{"Version":"2.0.0"},{"Version":"2.1.0"}],"url":"https://github.com/ClickHouse/clickhouse-go"},{"Releases":[],"url":"https://github.com/grafana/clickhouse"}]}`, toJson(event))
}

func TestJSONTypedSlice(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string][]int64{
		"random": {2, 3, 5},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	var event map[string][]int64
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(row1), toJson(event))
}

func TestJSONEscapeKeys(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := map[string][]int64{
		"56":      {1, 2, 3},
		"1.1":     {4, 5, 6},
		"世界世界世界":  {7, 8, 9},
		"1.1a":    {10, 11, 12},
		"a22.2":   {13, 14, 15},
		"a22`":    {16, 17, 18},
		"22.2`":   {19, 20, 21},
		"22.2\\`": {22, 23, 24},
		"s'":      {22, 23, 24},
		"a`a\\\\": {22, 23, 24},
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	var event map[string][]int64
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, toJson(row1), toJson(event))
}

func TestJSONChTags(t *testing.T) {
	type Event struct {
		Title        string `ch:"title"`
		Type         string
		Assignee     Account   `ch:"assignee"`
		Labels       []string  `ch:"labels"`
		Contributors []Account `ch:"-"`
		// should not be exported
		createdAt string
	}
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	row1 := Event{
		Title: "sample event",
		Type:  "event_a",
		Assignee: Account{
			Id:            1244,
			Name:          "Geoff",
			Achievement:   Achievement{Name: "Mars Star", AwardedDate: testDate.Truncate(time.Second)},
			Repositories:  []Repository{{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}, {URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}},
			Organizations: []string{"Support Engineer", "Integrations"},
		},
		Labels: []string{"Help wanted"},
		Contributors: []Account{
			{Id: 1244, Name: "Geoff", Achievement: Achievement{Name: "Mars Star", AwardedDate: testDate.Truncate(time.Second).Add(time.Hour * -3000)}, Repositories: []Repository{{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}, {URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}}, Organizations: []string{"Support Engineer", "Integrations"}},
			{Id: 2244, Achievement: Achievement{Name: "Managing S3 buckets", AwardedDate: testDate.Truncate(time.Second).Add(time.Hour * -500)}, Organizations: []string{"ClickHouse", "Consulting"}, Name: "Melyvn", Repositories: []Repository{{URL: "https://github.com/ClickHouse/support", Releases: []Releases{{Version: "1.0.0"}, {Version: "2.3.0"}, {Version: "2.3.0"}}}}},
		},
		createdAt: "2022-05-25 17:20:57 +0100 WEST",
	}
	require.NoError(t, batch.Append(row1))
	require.NoError(t, batch.Send())
	var event Event
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&event))
	assert.JSONEq(t, `{"Title":"sample event","Type":"event_a","Assignee":{"Id":1244,"Name":"Geoff","orgs":["Support Engineer","Integrations"],"Repositories":[{"url":"https://github.com/ClickHouse/clickhouse-python","Releases":[{"Version":"1.0.0"},{"Version":"1.1.0"}]},{"url":"https://github.com/ClickHouse/clickhouse-go","Releases":[{"Version":"2.0.0"},{"Version":"2.1.0"}]}],"Achievement":{"Name":"Mars Star","AwardedDate":"2022-05-25T17:20:57+01:00"}},"Labels":["Help wanted"],"Contributors":null}`, toJson(event))
}

func TestJSONFlush(t *testing.T) {
	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	vals := [1000]map[string]any{}
	for i := 0; i < 1000; i++ {
		vals[i] = map[string]any{
			"i": uint64(i),
			"s": RandAsciiString(10),
		}
		require.NoError(t, batch.Append(vals[i]))
		require.NoError(t, batch.Flush())
	}
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM json_test")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 map[string]any
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}

func TestMultipleJsonRowsWithNil(t *testing.T) {
	// will got new map to different order
	getMapByMapForTest := func(myMap map[string]any) map[string]any {
		newMap := map[string]any{}
		for k := range myMap {
			newMap[k] = myMap[k]
		}

		return newMap
	}

	type Login struct {
		Username   string `json:"username"`
		Attachment map[string]any
	}

	myAttachment := map[string]any{
		"col1": int64(1),
		"col2": time.Date(2022, 11, 21, 16, 21, 0, 0, time.Local),
		"col3": nil,
		"col4": "1",
	}

	conn, teardown := setupTest(t)
	defer teardown(t)
	ctx := context.Background()
	batch := prepareBatch(t, conn, ctx)
	for i := 0; i < 1000; i++ {
		row := Login{Username: "Gingerwizard", Attachment: getMapByMapForTest(myAttachment)}
		require.NoError(t, batch.Append(row))
	}

	require.NoError(t, batch.Send())
}
