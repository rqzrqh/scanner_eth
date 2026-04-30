package scan

import (
	"context"
	"fmt"
	"scanner_eth/blocktree"
	fetcherpkg "scanner_eth/fetch/fetcher"
	fetchserialstore "scanner_eth/fetch/serial_store"
	fetchstore "scanner_eth/fetch/store"
	fetchtask "scanner_eth/fetch/task"
	"scanner_eth/model"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

type testHeader = BlockHeaderJson

type testPendingBlockStore struct {
	nodeExists func(string) bool
	headers    map[string]*BlockHeaderJson
	bodies     map[string]*fetchstore.EventBlockData
}

func newTestPendingBlockStore(nodeExists func(string) bool) *testPendingBlockStore {
	return &testPendingBlockStore{
		nodeExists: nodeExists,
		headers:    make(map[string]*BlockHeaderJson),
		bodies:     make(map[string]*fetchstore.EventBlockData),
	}
}

func (a *testPendingBlockStore) SetNodeBlockHeader(hash string, header *BlockHeaderJson) bool {
	hash = normalizeTestHash(hash)
	if a == nil || a.nodeExists == nil || !a.nodeExists(hash) {
		return false
	}
	a.headers[hash] = header
	return true
}

func (a *testPendingBlockStore) SetNodeBlockBody(hash string, body *fetchstore.EventBlockData) bool {
	hash = normalizeTestHash(hash)
	if a == nil || a.nodeExists == nil || !a.nodeExists(hash) {
		return false
	}
	a.bodies[hash] = body
	return true
}

func (a *testPendingBlockStore) GetNodeBlockHeader(hash string) *BlockHeaderJson {
	return a.headers[normalizeTestHash(hash)]
}

func (a *testPendingBlockStore) GetNodeBlockBody(hash string) *fetchstore.EventBlockData {
	return a.bodies[normalizeTestHash(hash)]
}

func (a *testPendingBlockStore) DeleteBlock(hash string) {
	hash = normalizeTestHash(hash)
	if a == nil {
		return
	}
	delete(a.headers, hash)
	delete(a.bodies, hash)
}

type testFlowEnv struct {
	t *testing.T

	startHeight  uint64
	irreversible int
	latestRemote uint64

	blockTree *blocktree.BlockTree
	taskPool  *fetchtask.Pool
	pending   *testPendingBlockStore
	store     *fetchserialstore.Worker
	stored    *fetchstore.StoredBlockState

	fetchHeaderByHeightFn     func(context.Context, uint64) *BlockHeaderJson
	fetchHeaderByHashFn       func(context.Context, string) *BlockHeaderJson
	bootstrapHeaderByHeightFn func(context.Context, uint64) *BlockHeaderJson
	fetchBodyByHashFn         func(context.Context, string, uint64, *BlockHeaderJson) (*fetchstore.EventBlockData, int, int64, bool)
	updateNodeStateFn         func(int, int64, bool)
	triggerScanFn             func()

	flow *Flow
}

func newTestFlowEnv(t *testing.T, irreversible int) *testFlowEnv {
	t.Helper()

	env := &testFlowEnv{
		t:            t,
		startHeight:  1,
		irreversible: irreversible,
		blockTree:    blocktree.NewBlockTree(irreversible),
		taskPool:     &fetchtask.Pool{},
		store:        nil,
	}
	stored := fetchstore.NewStoredBlockState()
	env.stored = &stored
	t.Cleanup(func() {
		env.taskPool.Stop()
		if env.store != nil {
			env.store.Stop()
		}
	})
	env.pending = newTestPendingBlockStore(func(hash string) bool {
		return env.blockTree.Get(normalizeTestHash(hash)) != nil
	})

	env.flow = NewFlow(func() RuntimeDeps {
		return RuntimeDeps{
			StartHeight:  env.startHeight,
			Irreversible: env.irreversible,
			BlockTree:    env.blockTree,
			TaskPool:     env.taskPool,
			StoreWorker:  env.store,
			StoredBlocks: env.stored,
			TriggerScan: func() {
				if env.triggerScanFn != nil {
					env.triggerScanFn()
				}
			},
			PruneRuntime: PruneRuntimeDeps{
				BlockTree:         env.blockTree,
				PendingBlockStore: env.pending,
				StoredBlocks:      env.stored,
				TaskPool:          env.taskPool,
				NormalizeHash:     normalizeTestHash,
			},
			SetNodeBlockHeader: func(hash string, header *BlockHeaderJson) bool {
				return env.pending.SetNodeBlockHeader(hash, header)
			},
			SetNodeBlockBody: func(hash string, body *fetchstore.EventBlockData) bool {
				return env.pending.SetNodeBlockBody(hash, body)
			},
			GetNodeBlockHeader: func(hash string) *BlockHeaderJson {
				return env.pending.GetNodeBlockHeader(hash)
			},
			GetNodeBlockBody: func(hash string) *fetchstore.EventBlockData {
				return env.pending.GetNodeBlockBody(hash)
			},
			LatestRemoteHeight: func() uint64 {
				return env.latestRemote
			},
			BootstrapHeaderByHeight: func(ctx context.Context, height uint64) *BlockHeaderJson {
				if env.bootstrapHeaderByHeightFn != nil {
					return env.bootstrapHeaderByHeightFn(ctx, height)
				}
				return nil
			},
			FetchHeaderByHeight: func(ctx context.Context, height uint64) *BlockHeaderJson {
				if env.fetchHeaderByHeightFn != nil {
					return env.fetchHeaderByHeightFn(ctx, height)
				}
				return nil
			},
			FetchHeaderByHash: func(ctx context.Context, hash string) *BlockHeaderJson {
				if env.fetchHeaderByHashFn != nil {
					return env.fetchHeaderByHashFn(ctx, normalizeTestHash(hash))
				}
				return nil
			},
			FetchBodyByHash: func(ctx context.Context, hash string, height uint64, header *BlockHeaderJson) (*fetchstore.EventBlockData, int, int64, bool) {
				if env.fetchBodyByHashFn != nil {
					return env.fetchBodyByHashFn(ctx, normalizeTestHash(hash), height, header)
				}
				return nil, -1, 0, false
			},
			UpdateNodeState:  env.updateNodeStateFn,
			NormalizeHash:    normalizeTestHash,
			HeaderHeight:     testHeaderHeight,
			HeaderHash:       testHeaderHash,
			HeaderParentHash: testHeaderParentHash,
			HeaderWeight:     testHeaderWeight,
			BodyStorable: func(v *fetchstore.EventBlockData) bool {
				return v != nil && v.StorageFullBlock != nil
			},
		}
	}, Config{StartHeight: 1})

	return env
}

func (env *testFlowEnv) setStartHeight(height uint64) {
	env.startHeight = height
	env.flow.BindRuntimeDeps()
}

func (env *testFlowEnv) setIrreversible(irreversible int) {
	env.irreversible = irreversible
	env.flow.BindRuntimeDeps()
}

type testBlockStorer struct {
	storeFn func(context.Context, *fetchstore.EventBlockData) error
}

func (s *testBlockStorer) StoreBlockData(ctx context.Context, data *fetchstore.EventBlockData) error {
	if s != nil && s.storeFn != nil {
		return s.storeFn(ctx, data)
	}
	return nil
}

func (env *testFlowEnv) attachStoreWorker(storeFn func(context.Context, *fetchstore.EventBlockData) error) {
	env.t.Helper()
	if env.store != nil {
		env.store.Stop()
	}
	env.store = fetchserialstore.NewStartedWorker(&testBlockStorer{storeFn: storeFn}, env.stored, func(data *fetchstore.EventBlockData) bool {
		return data == nil || data.StorageFullBlock == nil
	})
	env.flow.BindRuntimeDeps()
}

func (env *testFlowEnv) attachAsyncTaskPool() {
	env.t.Helper()
	if env.taskPool != nil {
		env.taskPool.Stop()
	}
	pool := fetchtask.NewTaskPoolWithStop(
		fetchtask.TaskPoolOptions{WorkerCount: 1, HighQueueSize: 64, NormalQueueSize: 64, MaxRetry: 2},
		1,
		func(task *fetchtask.SyncTask, stopCh <-chan struct{}) bool {
			return HandleTaskPoolTask(env.flow, task, stopCh)
		},
	)
	env.taskPool = &pool
	env.flow.BindRuntimeDeps()
}

func (env *testFlowEnv) attachDefaultTaskDataFetchers() {
	env.t.Helper()
	if env.fetchHeaderByHashFn == nil {
		env.fetchHeaderByHashFn = func(_ context.Context, hash string) *BlockHeaderJson {
			node := env.blockTree.Get(normalizeTestHash(hash))
			if node == nil {
				return nil
			}
			return makeTestHeader(node.Height, node.Key, node.ParentKey)
		}
	}
	if env.fetchBodyByHashFn == nil {
		env.fetchBodyByHashFn = func(_ context.Context, hash string, height uint64, header *BlockHeaderJson) (*fetchstore.EventBlockData, int, int64, bool) {
			parent := ""
			if header != nil {
				parent = normalizeTestHash(header.ParentHash)
			}
			return makeTestEventBlockData(height, normalizeTestHash(hash), parent), -1, 0, true
		}
	}
	env.flow.BindRuntimeDeps()
}

func (env *testFlowEnv) runScanAndWait() {
	env.t.Helper()
	env.attachDefaultTaskDataFetchers()
	if env.taskPool == nil || env.taskPool.QueueHigh == nil || env.taskPool.QueueNormal == nil {
		env.attachAsyncTaskPool()
	}
	triggerCh := make(chan struct{}, 1)
	env.triggerScanFn = func() {
		select {
		case triggerCh <- struct{}{}:
		default:
		}
	}
	env.flow.BindRuntimeDeps()
	env.taskPool.Start()
	env.flow.RunScanCycle(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	quietSince := time.Time{}
	for time.Now().Before(deadline) {
		drainedTrigger := false
		for {
			select {
			case <-triggerCh:
				drainedTrigger = true
				env.flow.RunScanCycle(context.Background())
			default:
				goto checkIdle
			}
		}

	checkIdle:
		heightCount, hashCount := env.taskPool.HeaderSyncCounts()
		headerSyncing := heightCount > 0 || hashCount > 0
		if !headerSyncing && !drainedTrigger {
			if quietSince.IsZero() {
				quietSince = time.Now()
			} else if time.Since(quietSince) >= 100*time.Millisecond {
				return
			}
		} else {
			quietSince = time.Time{}
		}
		time.Sleep(5 * time.Millisecond)
	}

	env.t.Fatalf("scan stages did not finish before timeout")
}

func makeTestHeader(height uint64, hash string, parent string) *testHeader {
	return &testHeader{
		Number:     fmt.Sprintf("0x%x", height),
		Hash:       hash,
		ParentHash: parent,
		Difficulty: "0x1",
	}
}

func makeTestBody(storable bool) *fetchstore.EventBlockData {
	body := &fetchstore.EventBlockData{}
	if storable {
		body.StorageFullBlock = &fetchstore.StorageFullBlock{}
	}
	return body
}

func makeTestEventBlockData(height uint64, hash string, parent string) *fetchstore.EventBlockData {
	return &fetchstore.EventBlockData{
		StorageFullBlock: &fetchstore.StorageFullBlock{
			Block: model.Block{Height: height, Hash: hash, ParentHash: parent, Complete: true},
		},
	}
}

func snapshotTestStoredHashes(env *testFlowEnv) map[string]struct{} {
	if env == nil {
		return nil
	}
	env.t.Helper()
	if env.stored == nil {
		return nil
	}
	return env.stored.Snapshot()
}

func normalizeTestHash(hash string) string {
	if hash == "" {
		return ""
	}
	return strings.ToLower(hash)
}

func testHeaderHeight(h *testHeader) (uint64, bool) {
	if h == nil {
		return 0, false
	}
	height, err := hexutil.DecodeUint64(strings.TrimSpace(h.Number))
	return height, err == nil
}

func testHeaderHash(h *testHeader) string {
	if h == nil {
		return ""
	}
	return h.Hash
}

func testHeaderParentHash(h *testHeader) string {
	if h == nil {
		return ""
	}
	return h.ParentHash
}

func testHeaderWeight(h *testHeader) uint64 {
	return fetcherpkg.HeaderWeight(h)
}
