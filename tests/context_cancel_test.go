package tests

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestContextCancellationOfHeavyGeneratedInsert(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		SkipOnHTTP(t, protocol, "context cancel")

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

		conn, err := SetupTestContextCancellationType1(t, protocol, false)
		assert.Nil(t, err)
		assert.NotNil(t, conn)

		ExecuteTestContextCancellation(t, conn, heavyQuery)
	})
}

func TestContextCancellationOfHeavyOptimizeFinal(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		SkipOnHTTP(t, protocol, "context cancel")

		var (
			heavyQuery = "OPTIMIZE TABLE test_query_cancellation.trips FINAL"
		)

		conn, err := SetupTestContextCancellationType1(t, protocol, true)
		assert.Nil(t, err)
		assert.NotNil(t, conn)

		ExecuteTestContextCancellation(t, conn, heavyQuery)
	})
}

func TestContextCancellationOfHeavyInsertFromS3(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		SkipOnHTTP(t, protocol, "context cancel")

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

		conn, err := SetupTestContextCancellationType1(t, protocol, true)
		assert.Nil(t, err)
		assert.NotNil(t, conn)

		ExecuteTestContextCancellation(t, conn, heavyQuery)
	})
}

func SetupTestContextCancellationType1(t *testing.T, protocol clickhouse.Protocol, fillTableWithRandomData bool) (clickhouse.Conn, error) {
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

	conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
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

// TestContextCancellationNoConnectionSlotLeak verifies that when contexts are cancelled
// during connection acquisition, connection slots are properly released back to the pool.
// This test ensures that cancelled queries don't leak connection slots, which would
// eventually exhaust the connection pool.
func TestContextCancellationNoConnectionSlotLeak(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		env, err := GetNativeTestEnvironment()
		assert.Nil(t, err)

		// Select the correct port based on protocol
		port := env.Port
		if protocol == clickhouse.HTTP {
			port = env.HttpPort
		}

		// Create a connection with a very small pool size to make slot exhaustion obvious
		opts := &clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
			Auth: clickhouse.Auth{
				Database: env.Database,
				Username: env.Username,
				Password: env.Password,
			},
			MaxOpenConns: 2, // Small pool to make leaks obvious
			Protocol:     protocol,
		}

		conn, err := clickhouse.Open(opts)
		assert.Nil(t, err)
		assert.NotNil(t, conn)
		defer conn.Close()

		// Test that we can acquire connections repeatedly with cancelled contexts
		// without exhausting the connection pool
		const numAttempts = 10
		for i := 0; i < numAttempts; i++ {
			// Create a context that's already cancelled
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			// Try to execute a query with the cancelled context
			// This should fail but not leak a connection slot
			_ = conn.Exec(ctx, "SELECT 1")
		}

		// Now verify that we can still acquire connections successfully
		// If slots were leaked, this would hang or fail
		ctx := context.Background()
		err = conn.Exec(ctx, "SELECT 1")
		assert.Nil(t, err, "Should be able to execute query after cancelled context attempts")

		// Try to acquire both slots simultaneously to verify the pool is healthy
		done := make(chan error, 2)
		for i := 0; i < 2; i++ {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				done <- conn.Exec(ctx, "SELECT sleep(0.1)")
			}()
		}

		// Both should succeed without timing out
		for i := 0; i < 2; i++ {
			err := <-done
			assert.Nil(t, err, "Concurrent queries should succeed without slot exhaustion")
		}
	})
}
