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

// Election 领导者选举管理器
type Election struct {
	chainName   string
	redisClient *redis.Client
	rs          *redsync.Redsync
	mutex       *redsync.Mutex
}

// NewElection 创建领导者选举管理器
func NewElection(chainName string, redisClient *redis.Client) *Election {
	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)

	return &Election{
		chainName:   chainName,
		redisClient: redisClient,
		rs:          rs,
	}
}

// startWatchdog 启动看门狗，定期延长锁的租约
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

				// 延长租约
				if _, err := e.mutex.Extend(); err != nil {
					logrus.Infof("[%s] leader lease extend failed: %v", e.chainName, err)
					return
				}
			}
		}
	}()
}

// TryBecomeLeader 尝试成为领导者
func (e *Election) TryBecomeLeader(ctx context.Context, businessName string) bool {

	mutex := e.rs.NewMutex(
		fmt.Sprintf("%s:leader:%s", e.chainName, businessName),
		redsync.WithExpiry(10*time.Second),
		redsync.WithTries(1),
	)

	// 获取锁，如果获取失败（例如锁已被其他进程持有），会返回错误
	if err := mutex.LockContext(ctx); err != nil {
		return false
	}

	e.mutex = mutex
	return true
}

// IsStillLeader 检查是否仍然是领导者
func (e *Election) IsStillLeader() bool {
	if e.mutex == nil {
		return false
	}
	return time.Now().Add(500 * time.Millisecond).Before(e.mutex.Until())
}

// ReleaseLeader 释放领导者锁
func (e *Election) ReleaseLeader() {
	if e.mutex != nil {
		_, err := e.mutex.Unlock()
		if err != nil {
			logrus.Warnf("[%s] unlock failed: %v", e.chainName, err)
		}
		e.mutex = nil
	}
}

// RunAsLeader 作为领导者运行任务
func (e *Election) RunAsLeader(ctx context.Context, pollInterval time.Duration, executeAgain bool, task func(ctx context.Context) (bool, error)) {
	leaderCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	e.startWatchdog(leaderCtx)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-leaderCtx.Done():
			e.ReleaseLeader()
			return

		case <-ticker.C:
		ExecuteAgain:
			// 非常关键：确认自己仍然是 leader
			if !e.IsStillLeader() {
				e.ReleaseLeader()
				return
			}

			again, err := task(ctx)
			if err != nil {
				logrus.Errorf("[%s] task error: %v", e.chainName, err)
			} else {
				if executeAgain && again {
					goto ExecuteAgain
				}
			}
		}
	}
}

// DoWithLeaderElection 执行带领导者选举的任务
func (e *Election) DoWithLeaderElection(ctx context.Context, businessName string, pollInterval time.Duration, executeAgain bool, task func(ctx context.Context) (bool, error)) {
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

		logrus.Infof("[%s][%s] became relayer leader", e.chainName, businessName)

		e.RunAsLeader(ctx, pollInterval, executeAgain, task)

		logrus.Infof("[%s][%s] lost leadership, retrying", e.chainName, businessName)
	}
}
