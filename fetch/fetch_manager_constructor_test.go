package fetch

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"

	fetchtask "scanner_eth/fetch/task"
)

func TestNewFetchManagerSupportsInjectedOperators(t *testing.T) {
	db := newTestDB(t)
	redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { _ = redisClient.Close() })

	injectedDbOp := &mockDbOperator{}
	injectedFetcher := &mockBlockFetcher{}

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
		injectedFetcher,
	)

	if _, err := loadTestBlockWindowMaybeError(t, fm, context.Background()); err != nil {
		t.Fatalf("expected injected dbOperator to be used, got error: %v", err)
	}
	if fm.blockFetcher != injectedFetcher {
		t.Fatalf("expected injected blockFetcher to be used")
	}
}

func TestNewFetchManagerKeepsNilOperatorsWhenNilInjected(t *testing.T) {
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
	if fm.blockFetcher != nil {
		t.Fatalf("expected blockFetcher to remain nil when nil is injected")
	}
}
