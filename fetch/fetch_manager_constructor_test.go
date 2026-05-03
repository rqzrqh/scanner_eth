package fetch

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"

	fetcherpkg "scanner_eth/fetch/fetcher"
	fetchtask "scanner_eth/fetch/taskpool"
)

func TestNewFetchManagerSupportsMockOperators(t *testing.T) {
	db := newTestDB(t)
	redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { _ = redisClient.Close() })

	injectedDbOp := &mockDbOperator{}
	mockFetcher := fetcherpkg.NewMockFetcher(nil, nil, nil)

	fm := NewFetchManager(
		"test-chain",
		[]*ethclient.Client{},
		redisClient,
		1,
		0,
		2,
		0,
		fetchtask.TaskPoolOptions{},
		db,
		1,
		injectedDbOp,
		mockFetcher,
	)

	if _, err := loadTestBlockWindowMaybeError(t, fm, context.Background()); err != nil {
		t.Fatalf("expected mock db operator to be used, got error: %v", err)
	}
	if fm.fetcher == nil {
		t.Fatalf("expected mock fetcher to be used")
	}
}

func TestNewFetchManagerKeepsNilOperatorsWhenNilProvided(t *testing.T) {
	db := newTestDB(t)
	redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { _ = redisClient.Close() })

	fm := NewFetchManager(
		"test-chain",
		[]*ethclient.Client{},
		redisClient,
		1,
		0,
		2,
		0,
		fetchtask.TaskPoolOptions{},
		db,
		1,
		nil,
		nil,
	)

	if _, err := loadTestBlockWindowMaybeError(t, fm, context.Background()); err == nil {
		t.Fatalf("expected nil dbOperator to surface through db runtime deps")
	}
	if fm.fetcher != nil {
		t.Fatalf("expected fetcher to remain nil when nil is provided")
	}
}
