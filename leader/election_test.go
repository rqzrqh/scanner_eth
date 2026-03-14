package leader

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func newTestElection() *Election {
	return &Election{chainName: "test", forceLoseCh: make(chan struct{}, 1)}
}

func TestDoWithLeaderElectionContextCancelled(t *testing.T) {
	e := newTestElection()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		e.DoWithLeaderElection(ctx, "test", 50*time.Millisecond,
			func(ctx context.Context) error { return nil },
			func(ctx context.Context) error { return nil },
		)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("DoWithLeaderElection did not exit after context cancellation")
	}
}

func TestDoWithLeaderElectionBootstrapFailureThenSuccess(t *testing.T) {
	e := newTestElection()

	lockCalls := int32(0)
	isLeader := int32(1)
	released := int32(0)

	e.tryBecomeLeaderFn = func(ctx context.Context, name string) bool {
		atomic.AddInt32(&lockCalls, 1)
		return true
	}
	e.isStillLeaderFn = func() bool { return atomic.LoadInt32(&isLeader) == 1 }
	e.releaseLeaderFn = func() { atomic.AddInt32(&released, 1) }

	bootstrapCalls := int32(0)
	onLostCalls := int32(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		e.DoWithLeaderElection(ctx, "test", 10*time.Millisecond,
			func(ctx context.Context) error {
				n := atomic.AddInt32(&bootstrapCalls, 1)
				if n < 2 {
					return errors.New("bootstrap failed")
				}
				go func() {
					time.Sleep(30 * time.Millisecond)
					e.TriggerLostLeader()
				}()
				return nil
			},
			func(ctx context.Context) error {
				n := atomic.AddInt32(&onLostCalls, 1)
				if atomic.LoadInt32(&lockCalls) >= 3 && n >= 1 {
					cancel()
				}
				return nil
			},
		)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&onLostCalls) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if atomic.LoadInt32(&onLostCalls) == 0 {
		t.Fatal("onLostLeader should have run after TriggerLostLeader")
	}
	if atomic.LoadInt32(&bootstrapCalls) < 2 {
		t.Fatalf("bootstrap should have been called at least twice, got=%d", atomic.LoadInt32(&bootstrapCalls))
	}
	if atomic.LoadInt32(&released) == 0 {
		t.Fatal("lock should have been released at least once on bootstrap failure")
	}
}

func TestDoWithLeaderElectionOnLostLeaderCalledAfterRun(t *testing.T) {
	e := newTestElection()
	isLeader := int32(1)

	e.tryBecomeLeaderFn = func(ctx context.Context, name string) bool { return true }
	e.isStillLeaderFn = func() bool { return atomic.LoadInt32(&isLeader) == 1 }
	e.releaseLeaderFn = func() {}

	onLostCalls := int32(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		e.DoWithLeaderElection(ctx, "test", 10*time.Millisecond,
			func(ctx context.Context) error { return nil },
			func(ctx context.Context) error {
				atomic.AddInt32(&onLostCalls, 1)
				cancel()
				return nil
			},
		)
		close(done)
	}()

	time.Sleep(40 * time.Millisecond)
	e.TriggerLostLeader()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("DoWithLeaderElection did not exit after context cancellation")
	}

	if atomic.LoadInt32(&onLostCalls) != 1 {
		t.Fatalf("onLostLeader should be called once, got=%d", atomic.LoadInt32(&onLostCalls))
	}
}

func TestDoWithLeaderElectionSkipsRunWhenNoLongerLeaderBeforeRun(t *testing.T) {
	e := newTestElection()

	lockCalls := int32(0)
	leaderValid := int32(0)
	onLostCalls := int32(0)

	e.tryBecomeLeaderFn = func(ctx context.Context, name string) bool {
		atomic.AddInt32(&lockCalls, 1)
		return true
	}
	e.isStillLeaderFn = func() bool { return atomic.LoadInt32(&leaderValid) == 1 }
	e.releaseLeaderFn = func() {}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		e.DoWithLeaderElection(ctx, "test", 10*time.Millisecond,
			func(ctx context.Context) error {
				if atomic.LoadInt32(&lockCalls) >= 3 {
					atomic.StoreInt32(&leaderValid, 1)
					go func() {
						time.Sleep(20 * time.Millisecond)
						e.TriggerLostLeader()
					}()
				}
				return nil
			},
			func(ctx context.Context) error {
				n := atomic.AddInt32(&onLostCalls, 1)
				if atomic.LoadInt32(&lockCalls) >= 3 && n >= 1 {
					cancel()
				}
				return nil
			},
		)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&lockCalls) < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	if atomic.LoadInt32(&lockCalls) < 3 {
		t.Fatalf("expected at least 3 lock attempts, got=%d", atomic.LoadInt32(&lockCalls))
	}
	if atomic.LoadInt32(&onLostCalls) == 0 {
		t.Fatal("onLostLeader should run after leadership becomes valid and then lost")
	}
}

func TestTriggerLostLeaderReleasesLockImmediately(t *testing.T) {
	e := newTestElection()
	released := int32(0)
	e.releaseLeaderFn = func() { atomic.AddInt32(&released, 1) }

	e.TriggerLostLeader()
	if atomic.LoadInt32(&released) != 1 {
		t.Fatalf("expected releaseLeaderFn to be called once, got=%d", atomic.LoadInt32(&released))
	}
}
