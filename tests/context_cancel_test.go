package tests

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
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

		useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
		require.NoError(t, err)

		// Select the correct port based on protocol
		port := env.Port
		var tlsConfig *tls.Config
		if useSSL {
			tlsConfig = &tls.Config{}
		}
		switch {
		case protocol == clickhouse.HTTP && useSSL:
			port = env.HttpsPort
		case protocol == clickhouse.HTTP && !useSSL:
			port = env.HttpPort
		case protocol == clickhouse.Native && useSSL:
			port = env.SslPort
		case protocol == clickhouse.Native && !useSSL:
			port = env.Port
		}

		// Create a connection with a very small pool size to make slot exhaustion obvious
		opts := &clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
			Auth: clickhouse.Auth{
				Database: env.Database,
				Username: env.Username,
				Password: env.Password,
			},
			MaxOpenConns:    2,                 // Small pool to make leaks obvious
			ConnMaxLifetime: 100 * time.Second, // make it explicitly larger to avoid incidentally closing it
			MaxIdleConns:    5,                 // there can be max 5 connections on the pool
			Protocol:        protocol,
			TLS:             tlsConfig,
		}

		conn, err := clickhouse.Open(opts)
		assert.Nil(t, err)
		assert.NotNil(t, conn)
		defer conn.Close()

		t.Run("context already cancelled during acquire", func(t *testing.T) {
			// Test that we can acquire connections repeatedly with cancelled contexts
			// without exhausting the connection pool

			// Create a context that's already cancelled
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			const N = 10
			for range N {
				// Try to execute a query with the cancelled context
				// This should fail(because ctx is checked before getting it from connection pool)
				// but not leak a connection slot
				err = conn.Exec(ctx, "SELECT 1")
				require.ErrorIs(t, err, context.Canceled)
			}
			stats := conn.Stats()
			// no connection should be moved to pool as context is cancelled even before new connection is created
			assert.Equal(t, 0, stats.Idle)
			assert.Equal(t, 0, stats.Open)
		})

		t.Run("context cancelled during idle.Get", func(t *testing.T) {
			// Test scenario: Context cancelled AFTER writing to ch.open but DURING idle.Get()
			// To trigger this: saturate the pool first, then try to acquire with very short timeouts

			// Saturate the pool by running long queries in goroutines to hold both connection slots
			ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel1()

			ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel2()

			started := make(chan struct{}, 2)
			var saturateWg sync.WaitGroup
			saturateWg.Add(2)

			// Start first query in goroutine - Exec holds connection until complete
			go func() {
				defer saturateWg.Done()
				started <- struct{}{} // Signal we're about to execute
				err := conn.Exec(ctx1, "SELECT sleep(3)")
				require.ErrorIs(t, err, context.Canceled) // it's ctx1's cancel is called later
			}()

			// Start second query in goroutine - Exec holds connection until complete
			go func() {
				defer saturateWg.Done()
				started <- struct{}{} // Signal we're about to execute
				err = conn.Exec(ctx2, "SELECT sleep(3)")
				require.ErrorIs(t, err, context.Canceled) // it's ctx2's cancel is called later
			}()

			// Wait for both goroutines to start
			<-started
			<-started

			// Give queries time to start executing and hold connections
			time.Sleep(200 * time.Millisecond)

			// Now both connection slots should be occupied
			stats := conn.Stats()
			assert.Equal(t, 2, stats.Open, "both connection slots should be in use")
			assert.Equal(t, 0, stats.Idle, "no idle connections while both are in use")

			// Now try to acquire with very short timeouts
			// These will:
			// 1. Write to ch.open successfully (blocking until timeout or slot available)
			// 2. Timeout while waiting in idle.Get() for a connection to become available
			// 3. Must clean up ch.open slot to avoid leak
			const numAttempts = 5
			var wg sync.WaitGroup
			errChan := make(chan error, numAttempts)

			for range numAttempts {
				wg.Add(1)
				go func() {
					defer wg.Done()
					// Very short timeout - will timeout while waiting for a connection
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
					defer cancel()

					err := conn.Exec(ctx, "SELECT 1")
					errChan <- err
				}()
			}

			wg.Wait()
			close(errChan)

			// All attempts should have timed out
			for e := range errChan {
				require.Error(t, e)
				// Should be either DeadlineExceeded or Canceled (both indicate timeout)
				assert.True(t,
					errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled),
					"expected timeout errors, got: %v", e)
			}

			// Most importantly: verify no connection slots leaked
			// Cancel the saturating queries and wait for them to complete
			cancel1()
			cancel2()
			saturateWg.Wait()

			// Give a moment for connections to be released back to the pool
			time.Sleep(100 * time.Millisecond)

			// Verify we can still acquire connections successfully (pool is healthy, no leaks)
			err = conn.Exec(context.Background(), "SELECT 1")
			assert.NoError(t, err, "should be able to execute query after timeout attempts - no leaks")

			// Verify both slots work concurrently
			done := make(chan error, 2)
			for range 2 {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					done <- conn.Exec(ctx, "SELECT 1")
				}()
			}

			// Both should succeed without timing out
			for range 2 {
				err := <-done
				assert.NoError(t, err, "concurrent queries should succeed - no slot exhaustion")
			}
		})
	})
}
