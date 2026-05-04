package taskprocess

import (
	"context"
	"scanner_eth/blocktree"
	convertpkg "scanner_eth/fetch/convert"
	fetcherpkg "scanner_eth/fetch/fetcher"
	nodepkg "scanner_eth/fetch/node"
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/util"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
)

type RuntimeDeps struct {
	BlockTree       *blocktree.BlockTree
	StagingStore    *fetchstore.StagingStore
	NodeManager     *nodepkg.NodeManager
	Fetcher         fetcherpkg.Fetcher
	EnqueueBodyTask func(string)

	TryClaimHeaderHeight func(uint64) bool
	ReleaseHeaderHeight  func(uint64)
	TryClaimHeaderHash   func(string) bool
	ReleaseHeaderHash    func(string)
}

func NewRuntimeDeps(
	blockTree *blocktree.BlockTree,
	stagingStore *fetchstore.StagingStore,
	nodeManager *nodepkg.NodeManager,
	fetcher fetcherpkg.Fetcher,
	enqueueBodyTask func(string),
	tryClaimHeaderHeight func(uint64) bool,
	releaseHeaderHeight func(uint64),
	tryClaimHeaderHash func(string) bool,
	releaseHeaderHash func(string),
) RuntimeDeps {
	return RuntimeDeps{
		BlockTree:            blockTree,
		StagingStore:         stagingStore,
		NodeManager:          nodeManager,
		Fetcher:              fetcher,
		EnqueueBodyTask:      enqueueBodyTask,
		TryClaimHeaderHeight: tryClaimHeaderHeight,
		ReleaseHeaderHeight:  releaseHeaderHeight,
		TryClaimHeaderHash:   tryClaimHeaderHash,
		ReleaseHeaderHash:    releaseHeaderHash,
	}
}

func (deps RuntimeDeps) nodeExists(hash string) bool {
	hash = util.NormalizeHash(hash)
	return hash != "" && deps.BlockTree.Get(hash) != nil
}

func (deps RuntimeDeps) setPendingHeader(hash string, header *fetcherpkg.BlockHeaderJson) {
	hash = util.NormalizeHash(hash)
	if !deps.nodeExists(hash) {
		return
	}
	deps.StagingStore.SetPendingHeader(hash, header)
}

func (deps RuntimeDeps) setPendingBody(hash string, body *fetchstore.EventBlockData) {
	hash = util.NormalizeHash(hash)
	if !deps.nodeExists(hash) {
		return
	}
	deps.StagingStore.SetPendingBody(hash, body)
}

func (deps RuntimeDeps) getPendingHeader(hash string) *fetcherpkg.BlockHeaderJson {
	return deps.StagingStore.GetPendingHeader(util.NormalizeHash(hash))
}

func (deps RuntimeDeps) FetchHeaderByHeight(ctx context.Context, height uint64) *fetcherpkg.BlockHeaderJson {
	if ctx == nil {
		ctx = context.Background()
	}
	nodeID, nodeOp, err := deps.NodeManager.GetBestNode(height)
	if err != nil {
		return nil
	}
	startedAt := time.Now()
	header := deps.Fetcher.FetchBlockHeaderByHeight(ctx, nodeOp, 0, height)
	if header != nil {
		deps.NodeManager.RecordNodeResult(nodeID, time.Since(startedAt).Microseconds(), true)
	}
	return header
}

func (deps RuntimeDeps) FetchHeaderByHash(ctx context.Context, hash string) *fetcherpkg.BlockHeaderJson {
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	nodeID, nodeOp, err := deps.NodeManager.GetBestNode(0)
	if err != nil {
		return nil
	}
	startedAt := time.Now()
	header := deps.Fetcher.FetchBlockHeaderByHash(ctx, nodeOp, 0, hash)
	if header != nil {
		deps.NodeManager.RecordNodeResult(nodeID, time.Since(startedAt).Microseconds(), true)
	}
	return header
}

func (deps RuntimeDeps) fetchBodyByHash(ctx context.Context, hash string, height uint64, header *fetcherpkg.BlockHeaderJson) (body *fetchstore.EventBlockData, attempts []fetcherpkg.FullBlockRPCAttempt, ok bool) {
	nodeOps := deps.NodeManager.GetAllValidNodeOperators(height, hash)
	if len(nodeOps) == 0 {
		logrus.Warnf("body sync no valid nodes. height:%v hash:%v", height, hash)
		return nil, nil, false
	}
	nodeIDs := nodeOperatorIDs(nodeOps)
	logrus.Infof("body sync start. height:%v hash:%v valid_nodes:%v node_ids:%v", height, hash, len(nodeOps), nodeIDs)
	startTime := time.Now()
	result := deps.Fetcher.FetchFullBlockWithAttempts(ctx, nodeOps, int(height), header)
	cost := time.Since(startTime).Microseconds()
	if result != nil {
		attempts = result.Attempts
	}
	if result == nil || result.FullBlock == nil {
		logrus.Warnf("body sync failed. height:%v hash:%v valid_nodes:%v node_ids:%v cost_us:%v", height, hash, len(nodeOps), nodeIDs, cost)
		return nil, attempts, false
	}
	txCount := 0
	if result.FullBlock.Block != nil {
		txCount = result.FullBlock.Block.TxCount
	}
	logrus.Infof("body sync success. height:%v hash:%v valid_nodes:%v node_ids:%v txs:%v cost_us:%v", height, hash, len(nodeOps), nodeIDs, txCount, cost)
	irreversibleNode := blocktree.IrreversibleNode{}
	if treeNode := deps.BlockTree.Get(util.NormalizeHash(hash)); treeNode != nil {
		irreversibleNode = treeNode.Irreversible
	}
	return &fetchstore.EventBlockData{
		StorageFullBlock: convertpkg.ConvertStorageFullBlock(result.FullBlock, irreversibleNode),
	}, attempts, true
}

func nodeOperatorIDs(nodeOps []nodepkg.NodeOperator) string {
	if len(nodeOps) == 0 {
		return ""
	}
	ids := make([]string, 0, len(nodeOps))
	for _, nodeOp := range nodeOps {
		if nodeOp == nil {
			continue
		}
		ids = append(ids, strconv.Itoa(nodeOp.ID()))
	}
	return strings.Join(ids, ",")
}

func (deps RuntimeDeps) updateNodeState(id int, delay int64, success bool) {
	deps.NodeManager.UpdateNodeState(id, delay, success)
}

func (deps RuntimeDeps) updateNodeAttempts(attempts []fetcherpkg.FullBlockRPCAttempt) {
	for _, attempt := range attempts {
		if attempt.NodeID < 0 {
			continue
		}
		deps.updateNodeState(attempt.NodeID, attempt.CostMicros, attempt.Success)
	}
}

func headerHeight(header *fetcherpkg.BlockHeaderJson) (uint64, bool) {
	if header == nil {
		return 0, false
	}
	height, err := hexutil.DecodeUint64(strings.TrimSpace(header.Number))
	return height, err == nil
}

func (deps RuntimeDeps) InsertTreeHeader(header *fetcherpkg.BlockHeaderJson) {
	if header == nil {
		return
	}
	height, ok := headerHeight(header)
	if !ok {
		return
	}
	key := util.NormalizeHash(header.Hash)
	parentKey := util.NormalizeHash(header.ParentHash)
	weight := fetcherpkg.HeaderWeight(header)
	inserted := deps.BlockTree.Insert(height, key, parentKey, weight)
	if deps.EnqueueBodyTask == nil || len(inserted) == 0 {
		return
	}
	for _, node := range inserted {
		if node == nil {
			continue
		}
		hash := util.NormalizeHash(node.Key)
		if hash == "" {
			continue
		}
		deps.EnqueueBodyTask(hash)
	}
}

func (deps RuntimeDeps) SyncHeaderByHeight(ctx context.Context, height uint64) *fetcherpkg.BlockHeaderJson {
	header := deps.FetchHeaderByHeight(ctx, height)
	if header == nil {
		return nil
	}
	deps.InsertTreeHeader(header)
	deps.setPendingHeader(header.Hash, header)
	return header
}

func (deps RuntimeDeps) FetchAndInsertHeaderByHeight(height uint64) *fetcherpkg.BlockHeaderJson {
	if !deps.TryClaimHeaderHeight(height) {
		return nil
	}
	defer deps.ReleaseHeaderHeight(height)
	return deps.SyncHeaderByHeight(context.Background(), height)
}

func (deps RuntimeDeps) SyncHeaderByHash(ctx context.Context, hash string) bool {
	header := deps.FetchHeaderByHash(ctx, hash)
	if header == nil {
		return false
	}
	deps.InsertTreeHeader(header)
	deps.setPendingHeader(header.Hash, header)
	return true
}

func (deps RuntimeDeps) FetchAndInsertHeaderByHash(hash string) bool {
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return false
	}
	if !deps.TryClaimHeaderHash(hash) {
		return false
	}
	defer deps.ReleaseHeaderHash(hash)
	return deps.SyncHeaderByHash(context.Background(), hash)
}

func (deps RuntimeDeps) SyncNodeDataByHash(ctx context.Context, hash string) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return true
	}
	node := deps.BlockTree.Get(hash)
	if node == nil {
		return true
	}

	header := deps.getPendingHeader(hash)
	if header == nil {
		header = deps.FetchHeaderByHash(ctx, hash)
		if header == nil {
			return false
		}
		deps.setPendingHeader(hash, header)
	}

	height, ok := headerHeight(header)
	if !ok {
		height = node.Height
	}

	body, attempts, ok := deps.fetchBodyByHash(ctx, hash, height, header)
	deps.updateNodeAttempts(attempts)
	if !ok {
		return false
	}

	deps.setPendingBody(hash, body)
	return true
}
