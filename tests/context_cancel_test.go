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
	"log"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestContextCancellationOfHeavyGeneratedInsert(t *testing.T) {
	var (
		heavyQuery = `INSERT INTO test_query_cancellation.trips
			SELECT
				number + 1 AS trip_id,
				now() - INTERVAL intDiv(number, 100) SECOND AS pickup_datetime,
				now() - INTERVAL intDiv(number, 100) SECOND + INTERVAL rand() % 3600 SECOND AS dropoff_datetime,
				if(rand() % 2 = 0, NULL, (rand() % 3600) / 100.0 - 74.00) AS pickup_longitude,
				if(rand() % 2 = 0, NULL, (rand() % 3600) / 100.0 + 40.50) AS pickup_latitude,
				if(rand() % 2 = 0, NULL, (rand() % 3600) / 100.0 - 74.00) AS dropoff_longitude,
				if(rand() % 2 = 0, NULL, (rand() % 3600) / 100.0 + 40.50) AS dropoff_latitude,
				rand() % 6 + 1 AS passenger_count,
				(rand() % 2000) / 100.0 AS trip_distance,
				(rand() % 5000) / 100.0 AS fare_amount,
				(rand() % 500) / 100.0 AS extra,
				(rand() % 1000) / 100.0 AS tip_amount,
				(rand() % 300) / 100.0 AS tolls_amount,
				(rand() % 6000) / 100.0 AS total_amount,
				CAST(rand() % 5 + 1 AS Enum('CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4, 'UNK' = 5)) AS payment_type,
				'Neighborhood ' || toString(rand() % 100 + 1) AS pickup_ntaname,
				'Neighborhood ' || toString(rand() % 100 + 1) AS dropoff_ntaname
			FROM numbers(100000000);`
	)

	conn, err := SetupTestContextCancellationType1(t, false)
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	ExecuteTestContextCancellation(t, conn, heavyQuery)
}

func TestContextCancellationOfHeavyOptimizeFinal(t *testing.T) {
	var (
		heavyQuery = "OPTIMIZE TABLE test_query_cancellation.trips FINAL"
	)

	conn, err := SetupTestContextCancellationType1(t, true)
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	ExecuteTestContextCancellation(t, conn, heavyQuery)
}

func TestContextCancellationOfHeavyInsertFromS3(t *testing.T) {
	var (
		heavyQuery = `INSERT INTO test_query_cancellation.trips
		SELECT
			trip_id,
			pickup_datetime,
			dropoff_datetime,
			pickup_longitude,
			pickup_latitude,
			dropoff_longitude,
			dropoff_latitude,
			passenger_count,
			trip_distance,
			fare_amount,
			extra,
			tip_amount,
			tolls_amount,
			total_amount,
			payment_type,
			pickup_ntaname,
			dropoff_ntaname
		FROM s3(
			'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_{0..2}.gz',
			'TabSeparatedWithNames'
		);`
	)

	conn, err := SetupTestContextCancellationType1(t, true)
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	ExecuteTestContextCancellation(t, conn, heavyQuery)
}

func SetupTestContextCancellationType1(t *testing.T, fillTableWithRandomData bool) (clickhouse.Conn, error) {
	var (
		q1 = "CREATE DATABASE IF NOT EXISTS test_query_cancellation"
		q2 = "DROP TABLE IF EXISTS test_query_cancellation.trips"
		q3 = `CREATE TABLE test_query_cancellation.trips (
			trip_id             UInt32,
			pickup_datetime     DateTime,
			dropoff_datetime    DateTime,
			pickup_longitude    Nullable(Float64),
			pickup_latitude     Nullable(Float64),
			dropoff_longitude   Nullable(Float64),
			dropoff_latitude    Nullable(Float64),
			passenger_count     UInt8,
			trip_distance       Float32,
			fare_amount         Float32,
			extra               Float32,
			tip_amount          Float32,
			tolls_amount        Float32,
			total_amount        Float32,
			payment_type        Enum('CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4, 'UNK' = 5),
			pickup_ntaname      LowCardinality(String),
			dropoff_ntaname     LowCardinality(String)
		)
		ENGINE = MergeTree
		PRIMARY KEY (pickup_datetime, dropoff_datetime);`
		q4 = `INSERT INTO test_query_cancellation.trips
			SELECT
				number + 1 AS trip_id,
				now() - INTERVAL intDiv(number, 100) SECOND AS pickup_datetime,
				now() - INTERVAL intDiv(number, 100) SECOND + INTERVAL rand() % 3600 SECOND AS dropoff_datetime,
				if(rand() % 2 = 0, NULL, (rand() % 3600) / 100.0 - 74.00) AS pickup_longitude,
				if(rand() % 2 = 0, NULL, (rand() % 3600) / 100.0 + 40.50) AS pickup_latitude,
				if(rand() % 2 = 0, NULL, (rand() % 3600) / 100.0 - 74.00) AS dropoff_longitude,
				if(rand() % 2 = 0, NULL, (rand() % 3600) / 100.0 + 40.50) AS dropoff_latitude,
				rand() % 6 + 1 AS passenger_count,
				(rand() % 2000) / 100.0 AS trip_distance,
				(rand() % 5000) / 100.0 AS fare_amount,
				(rand() % 500) / 100.0 AS extra,
				(rand() % 1000) / 100.0 AS tip_amount,
				(rand() % 300) / 100.0 AS tolls_amount,
				(rand() % 6000) / 100.0 AS total_amount,
				CAST(rand() % 5 + 1 AS Enum('CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4, 'UNK' = 5)) AS payment_type,
				'Neighborhood ' || toString(rand() % 100 + 1) AS pickup_ntaname,
				'Neighborhood ' || toString(rand() % 100 + 1) AS dropoff_ntaname
			FROM numbers(30000000);`
	)

	prepareQueries := []string{q1, q2, q3}
	if fillTableWithRandomData {
		prepareQueries = append(prepareQueries, q4)
	}

	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})

	assert.Nil(t, err)
	assert.NotNil(t, conn)

	if err = conn.Ping(context.Background()); err != nil {
		return nil, err
	}

	t.Log("Connected.")

	// prepare table
	for _, query := range prepareQueries {
		err = conn.Exec(context.Background(), query)
		if err != nil {
			log.Printf("Finished with error: %v\n", err)
			conn.Close()
			return nil, err
		}
	}

	return conn, nil
}

func ExecuteTestContextCancellation(t *testing.T, conn clickhouse.Conn, query string) {
	// prepare context
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	doneCh := make(chan bool, 1)
	queryTimeCh := make(chan time.Duration, 1)

	// run query in background
	go func() {
		// running heavy query...
		start := time.Now()
		defer func() {
			queryTimeCh <- time.Since(start)
			doneCh <- true
		}()

		if err := conn.Exec(ctx, query); err != nil {
			return
		}
	}()

	cancelBackoff := 3 * time.Second

	// let query run for awhile and stop
	go func() {
		time.Sleep(3 * time.Second)
		cancelCtx()
	}()

	<-doneCh
	conn.Close()

	queryTime := <-queryTimeCh

	assert.Less(t, queryTime-cancelBackoff, time.Second)
}
