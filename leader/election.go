package leader

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Election manages leader election (Redis redsync).
type Election struct {
	chainName   string
	redisClient *redis.Client
	rs          *redsync.Redsync
	mutex       *redsync.Mutex
	forceLoseCh chan struct{}

	// hook fields – non-nil hooks override the default Redis-backed behaviour;
	// intended for unit tests only.
	tryBecomeLeaderFn func(ctx context.Context, name string) bool
	isStillLeaderFn   func() bool
	releaseLeaderFn   func()
	extendLeaseFn     func() bool
}

// NewElection constructs an Election for chainName using redisClient.
func NewElection(chainName string, redisClient *redis.Client) *Election {
	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)

	return &Election{
		chainName:   chainName,
		redisClient: redisClient,
		rs:          rs,
		forceLoseCh: make(chan struct{}, 1),
	}
}

// startWatchdog periodically extends the distributed lock lease.
func (e *Election) startWatchdog(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				if e.mutex == nil {
					return
				}

				// Renew lease.
				if e.extendLeaseFn != nil {
					if !e.extendLeaseFn() {
						logrus.Infof("[%s] leader lease extend failed (hook)", e.chainName)
						return
					}
				} else if _, err := e.mutex.Extend(); err != nil {
					logrus.Infof("[%s] leader lease extend failed: %v", e.chainName, err)
					return
				}
			}
		}
	}()
}

// TryBecomeLeader tries to acquire the leader lock.
func (e *Election) TryBecomeLeader(ctx context.Context, businessName string) bool {
	if e.tryBecomeLeaderFn != nil {
		return e.tryBecomeLeaderFn(ctx, businessName)
	}

	mutex := e.rs.NewMutex(
		fmt.Sprintf("%s:leader:%s", e.chainName, businessName),
		redsync.WithExpiry(10*time.Second),
		redsync.WithTries(1),
	)

	// Lock acquisition fails if another process holds the lock.
	if err := mutex.LockContext(ctx); err != nil {
		return false
	}

	e.mutex = mutex
	return true
}

// IsStillLeader checks whether the local lock lease is still valid.
func (e *Election) IsStillLeader() bool {
	if e.isStillLeaderFn != nil {
		return e.isStillLeaderFn()
	}
	if e.mutex == nil {
		return false
	}
	return e.mutex.Until().After(time.Now())
}

// ReleaseLeader releases the leader lock.
func (e *Election) ReleaseLeader() {
	if e.releaseLeaderFn != nil {
		e.releaseLeaderFn()
		return
	}
	if e.mutex != nil {
		_, err := e.mutex.Unlock()
		if err != nil {
			logrus.Warnf("[%s] unlock failed: %v", e.chainName, err)
		}
		e.mutex = nil
	}
}

// TriggerLostLeader forces loss of leadership: releases the lock if held and signals the election loop.
func (e *Election) TriggerLostLeader() {
	e.ReleaseLeader()
	if e.forceLoseCh == nil {
		return
	}
	select {
	case e.forceLoseCh <- struct{}{}:
	default:
	}
}

// DoWithLeaderElection runs a loop: acquire leadership, call onBecameLeader, then onLostLeader when leadership ends.
func (e *Election) DoWithLeaderElection(
	ctx context.Context,
	businessName string,
	pollInterval time.Duration,
	onBecameLeader func(ctx context.Context) error,
	onLostLeader func(ctx context.Context) error,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !e.TryBecomeLeader(ctx, businessName) {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if onBecameLeader != nil {
			if err := onBecameLeader(ctx); err != nil {
				logrus.Errorf("[%s][%s] leader bootstrap failed (releasing lock): %v", e.chainName, businessName, err)
				e.ReleaseLeader()
				time.Sleep(time.Second)
				continue
			}
		}

		logrus.Infof("[%s][%s] became relayer leader", e.chainName, businessName)
		e.waitUntilLeadershipLost(ctx, pollInterval)

		if onLostLeader != nil {
			onLostLeader(ctx)
		}

		if ctx.Err() != nil {
			return
		}

		logrus.Infof("[%s][%s] lost leadership, retrying", e.chainName, businessName)
	}
}

func (e *Election) waitUntilLeadershipLost(ctx context.Context, pollInterval time.Duration) {
	leaderCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	e.startWatchdog(leaderCtx)
	if pollInterval <= 0 {
		pollInterval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.ReleaseLeader()
			return
		case <-e.forceLoseCh:
			e.ReleaseLeader()
			return
		case <-ticker.C:
			if !e.IsStillLeader() {
				e.ReleaseLeader()
				return
			}
		}
	}
}
