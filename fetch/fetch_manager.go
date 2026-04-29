package fetch

import (
	"context"
	"expvar"
	"fmt"
	"scanner_eth/blocktree"
	convertpkg "scanner_eth/fetch/convert"
	fetcherpkg "scanner_eth/fetch/fetcher"
	headernotify "scanner_eth/fetch/header_notify"
	nodepkg "scanner_eth/fetch/node"
	fetchscan "scanner_eth/fetch/scan"
	fetchstore "scanner_eth/fetch/store"
	fetchtask "scanner_eth/fetch/task"
	"scanner_eth/leader"
	"scanner_eth/model"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type FetchManager struct {
	election               *leader.Election
	redisClient            *redis.Client
	redisMessageLoopCancel context.CancelFunc
	scannerMsgRedisBatch   int
	nodeManager            *nodepkg.NodeManager
	runtime                *fetchRuntimeState
	blockTree              *blocktree.BlockTree
	db                     *gorm.DB
	chainName              string
	chainId                int64
	irreversibleBlocks     int
	storedBlocks           fetchstore.StoredBlockState
	pendingPayloadStore    *fetchstore.PayloadStore[*BlockHeaderJson, *EventBlockData]
	taskPool               fetchtask.Pool
	hns                    []*headernotify.HeaderNotifier
	taskPoolOptions        fetchtask.TaskPoolOptions
	scanConfig             fetchscan.Config
	blockFetcher           fetcherpkg.BlockFetcher
	scanFlow               *fetchscan.Flow
	headerManager          *headernotify.Manager
	scanWorker             *fetchscan.Worker
	storeWorker            *fetchstore.SerialWorker[*EventBlockData]

	dbOperator DbOperator
}

type DbOperator interface {
	LoadBlockWindowFromDB(ctx context.Context) ([]model.Block, error)
	StoreBlockData(ctx context.Context, blockData *EventBlockData) error
}

type treePayloadAccessorAdapter struct {
	nodeExists func(string) bool
	store      *fetchstore.PayloadStore[*BlockHeaderJson, *EventBlockData]
}

func (a *treePayloadAccessorAdapter) SetNodeBlockHeader(hash string, header any) bool {
	if a == nil || a.store == nil || a.nodeExists == nil {
		return false
	}
	if !a.nodeExists(hash) {
		return false
	}
	typed, _ := header.(*BlockHeaderJson)
	a.store.SetHeader(hash, typed)
	return true
}

func (a *treePayloadAccessorAdapter) DeleteNodeBlockPayload(hash string) {
	if a == nil || a.store == nil {
		return
	}
	a.store.DeletePayload(hash)
}

func NewFetchManager(
	chainName string,
	clients []*ethclient.Client,
	redisClient *redis.Client,
	startHeight uint64,
	endHeight uint64,
	reversibleBlocks int,
	rpcTimeout time.Duration,
	taskPoolOptions fetchtask.TaskPoolOptions,
	db *gorm.DB,
	chainId int64,
	dbOperator DbOperator,
	blockFetcher fetcherpkg.BlockFetcher,
) *FetchManager {
	fetcherpkg.InitErc20Cache()
	fetcherpkg.InitErc721Cache()

	election := leader.NewElection(chainName, redisClient)

	hns := make([]*headernotify.HeaderNotifier, len(clients))
	for i, client := range clients {
		hns[i] = headernotify.NewHeaderNotifier(i, client)
	}

	_ = endHeight
	taskPoolOptions = fetchtask.NormalizeTaskPoolOptions(taskPoolOptions, len(clients))

	fm := &FetchManager{
		election:             election,
		redisClient:          redisClient,
		scannerMsgRedisBatch: defaultScannerMessageRedisBatch,
		nodeManager:          nodepkg.NewNodeManager(clients, rpcTimeout),
		db:                   db,
		chainName:            chainName,
		chainId:              chainId,
		irreversibleBlocks:   reversibleBlocks,
		hns:                  hns,
		taskPoolOptions: taskPoolOptions,
		scanConfig: fetchscan.Config{
			StartHeight: startHeight,
		},
		blockFetcher: blockFetcher,
		dbOperator:   dbOperator,
	}
	fm.createRuntimeState()
	return fm
}

func (fm *FetchManager) scanFlowRuntimeDeps() fetchscan.RuntimeDeps {
	if fm == nil {
		return fetchscan.RuntimeDeps{}
	}
	blockFetcher := fm.blockFetcher
	nodeManager := fm.nodeManager
	blockTree := fm.runtimeBlockTree()
	payloadStore := fm.runtimePayloadStore()
	nodeExists := func(hash string) bool {
		return blockTree != nil && blockTree.Get(hash) != nil
	}
	treeStoreDeps := fetchstore.TreeRuntimeDeps{
		BlockTree:       blockTree,
		PayloadAccessor: &treePayloadAccessorAdapter{nodeExists: nodeExists, store: payloadStore},
		StoredBlocks:    fm.runtimeStoredBlocks(),
		TaskPool:        fm.runtimeTaskPool(),
		NormalizeHash:   normalizeHash,
		ParseWeight:     parseStoredBlockWeight,
	}
	return fetchscan.RuntimeDeps{
		StartHeight:     fm.scanConfig.StartHeight,
		Irreversible:    fm.irreversibleBlocks,
		BlockTree:       blockTree,
		TaskPool:        fetchscan.NewTaskPoolAdapter(fm.runtimeTaskPool()),
		PayloadAccessor: fetchscan.NewPayloadStoreAccessor(nodeExists, payloadStore),
		StoreWorker:     fetchscan.NewStoreWorkerAdapter(fm.runtimeStoreWorker()),
		StoredBlocks:    treeStoreDeps.StoredBlocks,
		TriggerScan: func() {
			if scanWorker := fm.runtimeScanWorker(); scanWorker != nil {
				scanWorker.Trigger()
			}
		},
		PruneStoredBlocks: func(ctx context.Context, irreversible int) {
			treeStoreDeps.PruneStoredBlocks(ctx, irreversible)
		},
		LatestRemoteHeight: func() uint64 {
			if nodeManager == nil {
				return 0
			}
			return nodeManager.GetLatestHeight()
		},
		BootstrapHeaderByHeight: func(ctx context.Context, height uint64) any {
			if nodeManager == nil || blockFetcher == nil {
				return nil
			}
			for _, nodeOp := range nodeManager.NodeOperators() {
				if nodeOp == nil {
					continue
				}
				header := blockFetcher.FetchBlockHeaderByHeight(ctx, nodeOp, 0, height)
				if header != nil {
					return header
				}
			}
			return nil
		},
		FetchHeaderByHeight: func(ctx context.Context, height uint64) any {
			if nodeManager == nil || blockFetcher == nil {
				return nil
			}
			_, nodeOp, err := nodeManager.GetBestNode(height)
			if err != nil {
				return nil
			}
			return blockFetcher.FetchBlockHeaderByHeight(ctx, nodeOp, 0, height)
		},
		FetchHeaderByHash: func(ctx context.Context, hash string) any {
			if nodeManager == nil || blockFetcher == nil {
				return nil
			}
			_, nodeOp, err := nodeManager.GetBestNode(0)
			if err != nil {
				return nil
			}
			return blockFetcher.FetchBlockHeaderByHash(ctx, nodeOp, 0, normalizeHash(hash))
		},
		FetchBodyByHash: func(ctx context.Context, hash string, height uint64, header any) (body any, nodeID int, costMicros int64, ok bool) {
			if nodeManager == nil || blockFetcher == nil {
				return nil, -1, 0, false
			}
			h, _ := header.(*BlockHeaderJson)
			_, nodeOp, err := nodeManager.GetBestNode(height)
			if err != nil {
				return nil, -1, 0, false
			}
			startTime := time.Now()
			fullBlock := blockFetcher.FetchFullBlock(ctx, nodeOp, int(height), h)
			cost := time.Since(startTime).Microseconds()
			if fullBlock == nil {
				return nil, nodeOp.ID(), cost, false
			}
			irreversibleNode := blocktree.IrreversibleNode{}
			if blockTree != nil {
				if treeNode := blockTree.Get(normalizeHash(hash)); treeNode != nil {
					irreversibleNode = treeNode.Irreversible
				}
			}
			return &EventBlockData{
				StorageFullBlock: convertpkg.ConvertStorageFullBlock(fullBlock, irreversibleNode),
			}, nodeOp.ID(), cost, true
		},
		UpdateNodeState: func(id int, delay int64, success bool) {
			if nodeManager != nil {
				nodeManager.UpdateNodeState(id, delay, success)
			}
		},
		NormalizeHash: normalizeHash,
		HeaderExists: func(v any) bool {
			h, ok := v.(*BlockHeaderJson)
			return ok && h != nil
		},
		HeaderHeight: func(v any) (uint64, bool) {
			h, _ := v.(*BlockHeaderJson)
			if h == nil {
				return 0, false
			}
			height, err := hexutil.DecodeUint64(h.Number)
			return height, err == nil
		},
		HeaderHash: func(v any) string {
			h, _ := v.(*BlockHeaderJson)
			if h == nil {
				return ""
			}
			return h.Hash
		},
		HeaderParentHash: func(v any) string {
			h, _ := v.(*BlockHeaderJson)
			if h == nil {
				return ""
			}
			return h.ParentHash
		},
		HeaderWeight: func(v any) uint64 {
			h, _ := v.(*BlockHeaderJson)
			return fetcherpkg.HeaderWeight(h)
		},
		BodyExists: func(v any) bool {
			data, ok := v.(*EventBlockData)
			return ok && data != nil
		},
		BodyStorable: func(v any) bool {
			data, _ := v.(*EventBlockData)
			return data != nil && data.StorageFullBlock != nil
		},
	}
}

func (fm *FetchManager) Run() {
	if fm == nil {
		return
	}
	if fm.election == nil {
		return
	}
	go fm.election.DoWithLeaderElection(context.Background(), "scanEvents", time.Second, fm.onBecameLeader, fm.onLostLeader)
}

func (fm *FetchManager) Stop() {
	if fm == nil {
		return
	}
	fm.stopRuntimeWorkers()
	fm.stopRedisMessagePushLoop()
	if fm.election != nil {
		fm.election.TriggerLostLeader()
	}
	if taskPool := fm.runtimeTaskPool(); taskPool != nil {
		taskPool.Stop()
	}
}

func (fm *FetchManager) EnableTaskPoolMetrics(name string) {
	if strings.TrimSpace(name) == "" {
		name = "fetch_task_pool"
	}
	taskPool := fm.runtimeTaskPool()
	if taskPool == nil {
		return
	}
	taskPool.MetricsOnce.Do(func() {
		expvar.Publish(name, expvar.Func(func() any {
			return taskPool.MetricsPayload()
		}))
	})
	expvar.Publish(name+"_store_block", expvar.Func(func() any {
		storeWorker := fm.runtimeStoreWorker()
		if fm == nil || storeWorker == nil {
			return map[string]any{}
		}
		return storeWorker.MetricsPayload()
	}))
}

func (fm *FetchManager) onBecameLeader(ctx context.Context) error {
	fm.createRuntimeState()

	if fm.dbOperator == nil {
		return fmt.Errorf("db operator is nil")
	}
	blocks, err := fm.dbOperator.LoadBlockWindowFromDB(ctx)
	if err != nil {
		return err
	}

	loaded, err := fm.restoreBlockTree(blocks)
	if err != nil {
		logrus.Errorf("leader bootstrap load latest blocks from db failed: %v", err)
		return fmt.Errorf("leader bootstrap load latest blocks from db failed: %w", err)
	}

	if loaded > 0 {
		logrus.Infof("leader bootstrap from db success. loaded:%v", loaded)
	} else {
		scanFlow := fm.runtimeScanFlow()
		if scanFlow == nil || !scanFlow.EnsureBootstrapHeader() {
			logrus.Errorf("leader bootstrap from remote failed")
			return fmt.Errorf("leader bootstrap from remote failed")
		}

		logrus.Infof("leader bootstrap from remote success")
	}

	if headerManager := fm.runtimeHeaderManager(); headerManager != nil {
		headerManager.Start(context.Background())
	}

	if taskPool := fm.runtimeTaskPool(); taskPool != nil {
		taskPool.Start()
	}
	if scanWorker := fm.runtimeScanWorker(); scanWorker != nil {
		scanWorker.SetEnabled(true)
		scanWorker.Stop()
		scanWorker.Start()
	}
	fm.startRedisMessagePushLoop()

	return nil
}

func (fm *FetchManager) onLostLeader(ctx context.Context) error {
	_ = ctx
	if scanWorker := fm.runtimeScanWorker(); scanWorker != nil {
		scanWorker.SetEnabled(false)
	}
	fm.stopRuntimeWorkers()
	fm.stopRedisMessagePushLoop()
	if taskPool := fm.runtimeTaskPool(); taskPool != nil {
		taskPool.Stop()
	}
	fm.deleteRuntimeState()
	logrus.Infof("leader runtime state released")
	return nil
}
