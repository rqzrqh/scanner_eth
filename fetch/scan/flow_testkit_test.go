package scan

import (
	"context"
	"fmt"
	"scanner_eth/blocktree"
	"scanner_eth/data"
	fetcherpkg "scanner_eth/fetch/fetcher"
	nodepkg "scanner_eth/fetch/node"
	fetchserialstore "scanner_eth/fetch/serial_store"
	fetchstore "scanner_eth/fetch/store"
	fetchtaskprocess "scanner_eth/fetch/task_process"
	fetchtask "scanner_eth/fetch/taskpool"
	"scanner_eth/model"
	"scanner_eth/util"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
)

type testHeader = fetcherpkg.BlockHeaderJson

type storeOnlyOperator struct {
	storeFn func(context.Context, *fetchstore.EventBlockData) error
}

func (op storeOnlyOperator) StoreBlockData(ctx context.Context, blockData *fetchstore.EventBlockData) error {
	if op.storeFn == nil {
		return nil
	}
	return op.storeFn(ctx, blockData)
}

type testFlowEnv struct {
	t *testing.T

	startHeight  uint64
	irreversible int

	blockTree    *blocktree.BlockTree
	taskPool     *fetchtask.Pool
	stagingStore *fetchstore.StagingStore
	nodeManager  *nodepkg.NodeManager
	scanWorker   *Worker
	store        *fetchserialstore.Worker
	stored       *fetchstore.StoredBlockState

	fetchHeaderByHeightFn     func(context.Context, uint64) *fetcherpkg.BlockHeaderJson
	fetchHeaderByHashFn       func(context.Context, string) *fetcherpkg.BlockHeaderJson
	bootstrapHeaderByHeightFn func(context.Context, uint64) *fetcherpkg.BlockHeaderJson
	fetchBodyByHashFn         func(context.Context, string, uint64, *fetcherpkg.BlockHeaderJson) (*fetchstore.EventBlockData, int, int64, bool)

	flow *Flow
}

func testFetchHeaderByHeight(env *testFlowEnv, ctx context.Context, height uint64) *fetcherpkg.BlockHeaderJson {
	if env == nil {
		return nil
	}
	if env.fetchHeaderByHeightFn != nil {
		return env.fetchHeaderByHeightFn(ctx, height)
	}
	if env.bootstrapHeaderByHeightFn != nil {
		return env.bootstrapHeaderByHeightFn(ctx, height)
	}
	return nil
}

func testFetchHeaderByHash(env *testFlowEnv, ctx context.Context, hash string) *fetcherpkg.BlockHeaderJson {
	if env == nil || env.fetchHeaderByHashFn == nil {
		return nil
	}
	return env.fetchHeaderByHashFn(ctx, normalizeTestHash(hash))
}

func testFetchFullBlock(env *testFlowEnv, ctx context.Context, taskID int, header *fetcherpkg.BlockHeaderJson) *data.FullBlock {
	if env == nil || env.fetchBodyByHashFn == nil || header == nil {
		return nil
	}
	height := uint64(taskID)
	body, _, _, ok := env.fetchBodyByHashFn(ctx, normalizeTestHash(header.Hash), height, header)
	if !ok || body == nil || body.StorageFullBlock == nil {
		return nil
	}
	block := body.StorageFullBlock.Block
	return &data.FullBlock{
		Block: &data.Block{
			Height:     block.Height,
			Hash:       block.Hash,
			ParentHash: block.ParentHash,
		},
		StateSet: &data.StateSet{},
	}
}

func newTestFlowEnv(t *testing.T, irreversible int) *testFlowEnv {
	t.Helper()

	env := &testFlowEnv{
		t:            t,
		startHeight:  1,
		irreversible: irreversible,
		blockTree:    blocktree.NewBlockTree(irreversible),
		taskPool:     &fetchtask.Pool{},
		nodeManager:  nodepkg.NewNodeManager([]*ethclient.Client{nil}, 0),
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
	env.stagingStore = fetchstore.NewStagingStore()

	env.flow = NewFlow(func() RuntimeDeps {
		return RuntimeDeps{
			StartHeight:  env.startHeight,
			Irreversible: env.irreversible,
			BlockTree:    env.blockTree,
			TaskPool:     env.taskPool,
			StoreWorker:  env.store,
			StoredBlocks: env.stored,
			ScanWorker:   env.scanWorker,
			StagingStore: env.stagingStore,
			NodeManager:  env.nodeManager,
			Fetcher: fetcherpkg.NewMockFetcher(
				func(ctx context.Context, _ nodepkg.NodeOperator, _ int, height uint64) *fetcherpkg.BlockHeaderJson {
					return testFetchHeaderByHeight(env, ctx, height)
				},
				func(ctx context.Context, _ nodepkg.NodeOperator, _ int, hash string) *fetcherpkg.BlockHeaderJson {
					return testFetchHeaderByHash(env, ctx, hash)
				},
				func(ctx context.Context, _ nodepkg.NodeOperator, taskID int, header *fetcherpkg.BlockHeaderJson) *data.FullBlock {
					return testFetchFullBlock(env, ctx, taskID, header)
				},
			),
			PruneRuntime: PruneRuntimeDeps{
				BlockTree:    env.blockTree,
				StagingStore: env.stagingStore,
				StoredBlocks: env.stored,
				TaskPool:     env.taskPool,
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

func (env *testFlowEnv) setLatestRemote(height uint64) {
	env.t.Helper()
	if env.nodeManager == nil {
		return
	}
	env.nodeManager.ResetRemoteChainTips()
	if height == 0 {
		return
	}
	env.nodeManager.UpdateNodeChainInfo(0, height, fmt.Sprintf("0x%x", height))
}

func (env *testFlowEnv) insertLinearChain(hashes ...string) {
	env.t.Helper()
	parent := ""
	for i, hash := range hashes {
		env.blockTree.Insert(uint64(i+1), hash, parent, 1)
		parent = hash
	}
}

func (env *testFlowEnv) stagePendingNodes(hashes ...string) {
	env.t.Helper()
	for _, hash := range hashes {
		node := env.blockTree.Get(hash)
		if node == nil {
			env.t.Fatalf("expected blocktree node for %s", hash)
		}
		env.stagingStore.SetPendingHeader(hash, makeTestHeader(node.Height, hash, node.ParentKey))
		env.stagingStore.SetPendingBody(hash, makeTestEventBlockData(node.Height, hash, node.ParentKey))
	}
}

func (env *testFlowEnv) markStoredHashes(hashes ...string) {
	env.t.Helper()
	for _, hash := range hashes {
		env.stored.MarkStored(hash)
	}
}

func (env *testFlowEnv) seedStoredLinearBranch(hashes ...string) {
	env.t.Helper()
	if len(hashes) == 0 {
		return
	}
	env.insertLinearChain(hashes...)
	env.markStoredHashes(hashes[0])
	if len(hashes) > 1 {
		env.stagePendingNodes(hashes[1:]...)
	}
}

func (env *testFlowEnv) attachStoreWorker(storeFn func(context.Context, *fetchstore.EventBlockData) error) {
	env.t.Helper()
	if env.store != nil {
		env.store.Stop()
	}
	env.store = fetchserialstore.NewStartedWorker(storeOnlyOperator{storeFn: storeFn}, env.stored, func(data *fetchstore.EventBlockData) bool {
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
			return fetchtask.DispatchSyncTask(
				task,
				stopCh,
				func(hash string, stopCh <-chan struct{}) bool {
					return fetchtaskprocess.HandleBodyTask(env.flow.taskRuntime, hash, stopCh)
				},
				func(hash string) bool {
					return fetchtaskprocess.HandleHeaderHashTask(env.flow.taskRuntime, hash)
				},
				func(height uint64) bool {
					return fetchtaskprocess.HandleHeaderHeightTask(env.flow.taskRuntime, height)
				},
			)
		},
	)
	env.taskPool = &pool
	env.flow.BindRuntimeDeps()
}

func (env *testFlowEnv) attachDefaultTaskDataFetchers() {
	env.t.Helper()
	if env.fetchHeaderByHashFn == nil {
		env.fetchHeaderByHashFn = func(_ context.Context, hash string) *fetcherpkg.BlockHeaderJson {
			node := env.blockTree.Get(normalizeTestHash(hash))
			if node == nil {
				return nil
			}
			return makeTestHeader(node.Height, node.Key, node.ParentKey)
		}
	}
	if env.fetchBodyByHashFn == nil {
		env.fetchBodyByHashFn = func(_ context.Context, hash string, height uint64, header *fetcherpkg.BlockHeaderJson) (*fetchstore.EventBlockData, int, int64, bool) {
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
	if env.scanWorker == nil {
		env.scanWorker = &Worker{triggerCh: make(chan struct{}, 1)}
		env.scanWorker.SetEnabled(true)
	}
	triggerCh := env.scanWorker.TriggerChan()
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
		taskStats := env.taskPool.Stats()
		taskSyncing := heightCount > 0 || hashCount > 0 || taskStats.PendingHigh > 0 || taskStats.PendingNormal > 0
		storeBusy := env.store != nil && !env.store.IsIdle()
		if !taskSyncing && !storeBusy && !drainedTrigger {
			bodyWorkPending := false
			if env.store != nil {
				bodyWorkPending = env.flow.CountStoreBranchNodes() > 0
			}
			hasMoreWork := len(env.flow.GetExpandTreeTargets()) > 0 ||
				len(env.flow.GetFillTreeTargets()) > 0 ||
				bodyWorkPending
			if hasMoreWork {
				env.flow.RunScanCycle(context.Background())
				quietSince = time.Time{}
				env.waitRuntimeQuiet(20 * time.Millisecond)
				continue
			}
			if quietSince.IsZero() {
				quietSince = time.Now()
			} else if time.Since(quietSince) >= 100*time.Millisecond {
				return
			}
		} else {
			quietSince = time.Time{}
		}
		env.waitRuntimeQuiet(20 * time.Millisecond)
	}

	env.t.Fatalf("scan stages did not finish before timeout")
}

func (env *testFlowEnv) waitRuntimeQuiet(timeout time.Duration) {
	env.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = env.taskPool.WaitIdle(ctx, 0)
	if env.store != nil {
		_ = env.store.WaitIdle(ctx, 0)
	}
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
	return util.NormalizeHash(hash)
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
