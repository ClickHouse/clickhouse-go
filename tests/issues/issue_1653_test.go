package issues

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestIssue1653RandomDialStrategyVisitsEachAddressOnce(t *testing.T) {
	addresses := []string{"a", "b", "c", "d"}
	dialErr := errors.New("dial failed")

	for range 64 {
		var dialed []string
		_, err := clickhouse.DefaultDialStrategy(
			context.Background(),
			0,
			&clickhouse.Options{
				Addr:             addresses,
				ConnOpenStrategy: clickhouse.ConnOpenRandom,
			},
			func(_ context.Context, addr string, _ *clickhouse.Options) (clickhouse.DialResult, error) {
				dialed = append(dialed, addr)
				return clickhouse.DialResult{}, dialErr
			},
		)

		require.ErrorIs(t, err, dialErr)
		require.Len(t, dialed, len(addresses))
		assert.ElementsMatch(t, addresses, dialed)
		assert.True(t, isAddressRotation(addresses, dialed), "expected %v to be a rotation of %v", dialed, addresses)
	}
}

func TestIssue1653RoundRobinDialStrategyVisitsEachAddressOnce(t *testing.T) {
	addresses := []string{"a", "b", "c", "d"}
	dialErr := errors.New("dial failed")
	var dialed []string

	_, err := clickhouse.DefaultDialStrategy(
		context.Background(),
		1,
		&clickhouse.Options{
			Addr:             addresses,
			ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
		},
		func(_ context.Context, addr string, _ *clickhouse.Options) (clickhouse.DialResult, error) {
			dialed = append(dialed, addr)
			return clickhouse.DialResult{}, dialErr
		},
	)

	require.ErrorIs(t, err, dialErr)
	assert.Equal(t, []string{"b", "c", "d", "a"}, dialed)
}

func TestIssue1653RandomDialStrategyHandlesEmptyAddresses(t *testing.T) {
	called := false

	_, err := clickhouse.DefaultDialStrategy(
		context.Background(),
		0,
		&clickhouse.Options{ConnOpenStrategy: clickhouse.ConnOpenRandom},
		func(_ context.Context, _ string, _ *clickhouse.Options) (clickhouse.DialResult, error) {
			called = true
			return clickhouse.DialResult{}, nil
		},
	)

	require.ErrorIs(t, err, clickhouse.ErrAcquireConnNoAddress)
	assert.False(t, called)
}

func isAddressRotation(addresses, dialed []string) bool {
	if len(addresses) != len(dialed) {
		return false
	}

	for offset := range addresses {
		matches := true
		for index := range addresses {
			if dialed[index] != addresses[(offset+index)%len(addresses)] {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}

	return false
}
