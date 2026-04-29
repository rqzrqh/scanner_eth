package scan

import (
	"context"
	"fmt"
	"math/big"
	"scanner_eth/blocktree"
	"strconv"
	"strings"
	"testing"
)

type testHeader struct {
	Number     string
	Hash       string
	ParentHash string
	Difficulty string
}

type testBody struct {
	storable bool
}

type testTaskPool struct {
	headerHeightSyncing map[uint64]struct{}
	headerHashSyncing   map[string]struct{}
	enqueuedHeights     []uint64
	enqueuedHashes      []string
	bodyTasks           []testBodyTask
}

type testBodyTask struct {
	hash     string
	priority int
}

func newTestTaskPool() *testTaskPool {
	return &testTaskPool{
		headerHeightSyncing: make(map[uint64]struct{}),
		headerHashSyncing:   make(map[string]struct{}),
	}
}

func (p *testTaskPool) EnqueueHeaderHeightTask(height uint64) bool {
	p.enqueuedHeights = append(p.enqueuedHeights, height)
	return true
}

func (p *testTaskPool) EnqueueHeaderHashTask(hash string) bool {
	p.enqueuedHashes = append(p.enqueuedHashes, normalizeTestHash(hash))
	return true
}

func (p *testTaskPool) EnqueueBodyTask(hash string, priority int) {
	p.bodyTasks = append(p.bodyTasks, testBodyTask{hash: normalizeTestHash(hash), priority: priority})
}

func (p *testTaskPool) IsHeaderHeightSyncing(height uint64) bool {
	_, ok := p.headerHeightSyncing[height]
	return ok
}

func (p *testTaskPool) TryStartHeaderHeightSync(height uint64) bool {
	if p.IsHeaderHeightSyncing(height) {
		return false
	}
	p.headerHeightSyncing[height] = struct{}{}
	return true
}

func (p *testTaskPool) FinishHeaderHeightSync(height uint64) {
	delete(p.headerHeightSyncing, height)
}

func (p *testTaskPool) IsHeaderHashSyncing(hash string) bool {
	_, ok := p.headerHashSyncing[normalizeTestHash(hash)]
	return ok
}

func (p *testTaskPool) TryStartHeaderHashSync(hash string) bool {
	hash = normalizeTestHash(hash)
	if hash == "" || p.IsHeaderHashSyncing(hash) {
		return false
	}
	p.headerHashSyncing[hash] = struct{}{}
	return true
}

func (p *testTaskPool) FinishHeaderHashSync(hash string) {
	delete(p.headerHashSyncing, normalizeTestHash(hash))
}

type testPayloadAccessor struct {
	nodeExists func(string) bool
	headers    map[string]any
	bodies     map[string]any
}

func newTestPayloadAccessor(nodeExists func(string) bool) *testPayloadAccessor {
	return &testPayloadAccessor{
		nodeExists: nodeExists,
		headers:    make(map[string]any),
		bodies:     make(map[string]any),
	}
}

func (a *testPayloadAccessor) SetNodeBlockHeader(hash string, header any) bool {
	hash = normalizeTestHash(hash)
	if a == nil || a.nodeExists == nil || !a.nodeExists(hash) {
		return false
	}
	a.headers[hash] = header
	return true
}

func (a *testPayloadAccessor) SetNodeBlockBody(hash string, body any) bool {
	hash = normalizeTestHash(hash)
	if a == nil || a.nodeExists == nil || !a.nodeExists(hash) {
		return false
	}
	a.bodies[hash] = body
	return true
}

func (a *testPayloadAccessor) GetNodeBlockHeader(hash string) any {
	return a.headers[normalizeTestHash(hash)]
}

func (a *testPayloadAccessor) GetNodeBlockBody(hash string) any {
	return a.bodies[normalizeTestHash(hash)]
}

type testStoreWorker struct {
	inflight    map[string]struct{}
	submissions []testStoreSubmission
	submitFn    func(context.Context, string, uint64, any) error
}

type testStoreSubmission struct {
	hash   string
	height uint64
	data   any
}

func newTestStoreWorker() *testStoreWorker {
	return &testStoreWorker{inflight: make(map[string]struct{})}
}

func (w *testStoreWorker) IsInflight(hash string) bool {
	_, ok := w.inflight[normalizeTestHash(hash)]
	return ok
}

func (w *testStoreWorker) Submit(ctx context.Context, hash string, height uint64, data any) error {
	hash = normalizeTestHash(hash)
	w.submissions = append(w.submissions, testStoreSubmission{hash: hash, height: height, data: data})
	if w.submitFn != nil {
		return w.submitFn(ctx, hash, height, data)
	}
	return nil
}

type testStoredBlocks struct {
	hashes map[string]struct{}
}

func newTestStoredBlocks() *testStoredBlocks {
	return &testStoredBlocks{hashes: make(map[string]struct{})}
}

func (s *testStoredBlocks) IsStored(hash string) bool {
	_, ok := s.hashes[normalizeTestHash(hash)]
	return ok
}

func (s *testStoredBlocks) markStored(hash string) {
	hash = normalizeTestHash(hash)
	if hash == "" {
		return
	}
	s.hashes[hash] = struct{}{}
}

type testFlowEnv struct {
	t *testing.T

	startHeight  uint64
	irreversible int
	latestRemote uint64

	blockTree *blocktree.BlockTree
	taskPool  *testTaskPool
	payloads  *testPayloadAccessor
	store     *testStoreWorker
	stored    *testStoredBlocks

	fetchHeaderByHeightFn     func(context.Context, uint64) any
	fetchHeaderByHashFn       func(context.Context, string) any
	bootstrapHeaderByHeightFn func(context.Context, uint64) any
	fetchBodyByHashFn         func(context.Context, string, uint64, any) (any, int, int64, bool)
	updateNodeStateFn         func(int, int64, bool)

	flow *Flow
}

func newTestFlowEnv(t *testing.T, irreversible int) *testFlowEnv {
	t.Helper()

	env := &testFlowEnv{
		t:            t,
		startHeight:  1,
		irreversible: irreversible,
		blockTree:    blocktree.NewBlockTree(irreversible),
		taskPool:     newTestTaskPool(),
		store:        newTestStoreWorker(),
		stored:       newTestStoredBlocks(),
	}
	env.payloads = newTestPayloadAccessor(func(hash string) bool {
		return env.blockTree.Get(normalizeTestHash(hash)) != nil
	})

	env.flow = NewFlow(func() RuntimeDeps {
		return RuntimeDeps{
			StartHeight:       env.startHeight,
			Irreversible:      env.irreversible,
			BlockTree:         env.blockTree,
			TaskPool:          env.taskPool,
			PayloadAccessor:   env.payloads,
			StoreWorker:       env.store,
			StoredBlocks:      env.stored,
			TriggerScan:       func() {},
			PruneStoredBlocks: func(context.Context, int) {},
			LatestRemoteHeight: func() uint64 {
				return env.latestRemote
			},
			BootstrapHeaderByHeight: func(ctx context.Context, height uint64) any {
				if env.bootstrapHeaderByHeightFn != nil {
					return env.bootstrapHeaderByHeightFn(ctx, height)
				}
				return nil
			},
			FetchHeaderByHeight: func(ctx context.Context, height uint64) any {
				if env.fetchHeaderByHeightFn != nil {
					return env.fetchHeaderByHeightFn(ctx, height)
				}
				return nil
			},
			FetchHeaderByHash: func(ctx context.Context, hash string) any {
				if env.fetchHeaderByHashFn != nil {
					return env.fetchHeaderByHashFn(ctx, normalizeTestHash(hash))
				}
				return nil
			},
			FetchBodyByHash: func(ctx context.Context, hash string, height uint64, header any) (any, int, int64, bool) {
				if env.fetchBodyByHashFn != nil {
					return env.fetchBodyByHashFn(ctx, normalizeTestHash(hash), height, header)
				}
				return nil, -1, 0, false
			},
			UpdateNodeState: env.updateNodeStateFn,
			NormalizeHash:   normalizeTestHash,
			HeaderExists: func(v any) bool {
				h, ok := v.(*testHeader)
				return ok && h != nil
			},
			HeaderHeight:     testHeaderHeight,
			HeaderHash:       testHeaderHash,
			HeaderParentHash: testHeaderParentHash,
			HeaderWeight:     testHeaderWeight,
			BodyExists: func(v any) bool {
				b, ok := v.(*testBody)
				return ok && b != nil
			},
			BodyStorable: func(v any) bool {
				b, ok := v.(*testBody)
				return ok && b != nil && b.storable
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

func makeTestHeader(height uint64, hash string, parent string) *testHeader {
	return &testHeader{
		Number:     fmt.Sprintf("0x%x", height),
		Hash:       hash,
		ParentHash: parent,
		Difficulty: "0x1",
	}
}

func normalizeTestHash(hash string) string {
	if hash == "" {
		return ""
	}
	return strings.ToLower(hash)
}

func testHeaderHeight(v any) (uint64, bool) {
	h, ok := v.(*testHeader)
	if !ok || h == nil {
		return 0, false
	}
	height, err := strconv.ParseUint(strings.TrimSpace(h.Number), 0, 64)
	return height, err == nil
}

func testHeaderHash(v any) string {
	h, _ := v.(*testHeader)
	if h == nil {
		return ""
	}
	return h.Hash
}

func testHeaderParentHash(v any) string {
	h, _ := v.(*testHeader)
	if h == nil {
		return ""
	}
	return h.ParentHash
}

func testHeaderWeight(v any) uint64 {
	h, _ := v.(*testHeader)
	if h == nil {
		return 0
	}
	difficulty := strings.TrimSpace(h.Difficulty)
	if difficulty == "" {
		return 0
	}

	n := new(big.Int)
	if _, ok := n.SetString(difficulty, 0); !ok || n.Sign() <= 0 {
		return 0
	}

	max := new(big.Int).SetUint64(^uint64(0))
	if n.Cmp(max) > 0 {
		return ^uint64(0)
	}
	return n.Uint64()
}
