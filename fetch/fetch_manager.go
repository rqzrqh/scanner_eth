package fetch

import (
	"context"
	"expvar"
	"fmt"
	fetcherpkg "scanner_eth/fetch/fetcher"
	nodepkg "scanner_eth/fetch/node"
	fetchrestore "scanner_eth/fetch/restore"
	fetchscan "scanner_eth/fetch/scan"
	fetchstore "scanner_eth/fetch/store"
	fetchtask "scanner_eth/fetch/taskpool"
	"scanner_eth/leader"
	"scanner_eth/model"
	"strings"
	"time"

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
	db                     *gorm.DB
	chainName              string
	chainId                int64
	irreversibleBlocks     int
	taskPoolOptions        fetchtask.TaskPoolOptions
	scanConfig             fetchscan.Config

	dbOperator fetchstore.DBOperator
	fetcher    fetcherpkg.Fetcher
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
	dbOperator fetchstore.DBOperator,
	fetcher fetcherpkg.Fetcher,
) *FetchManager {
	fetcherpkg.InitErc20Cache()
	fetcherpkg.InitErc721Cache()

	election := leader.NewElection(chainName, redisClient)

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
		taskPoolOptions:      taskPoolOptions,
		scanConfig: fetchscan.Config{
			StartHeight: startHeight,
		},
		dbOperator: dbOperator,
		fetcher:    fetcher,
	}
	fm.createRuntimeState()
	return fm
}

func (fm *FetchManager) restoreBlockTree(blocks []model.Block) (int, error) {
	if fm == nil {
		return 0, nil
	}
	loaded, err := fetchrestore.RuntimeDeps{
		BlockTree:    fm.runtimeBlockTree(),
		StagingStore: fm.runtimeStagingStore(),
		StoredBlocks: fm.runtimeStoredBlocks(),
	}.RestoreBlockTree(blocks)
	if err != nil || loaded == 0 || fm.irreversibleBlocks <= 0 {
		return loaded, err
	}

	fm.scanFlowRuntimeDeps().PruneRuntime.PruneStoredBlocks(context.Background(), fm.irreversibleBlocks)
	return loaded, nil
}

func (fm *FetchManager) scanFlowRuntimeDeps() fetchscan.RuntimeDeps {
	if fm == nil {
		return fetchscan.RuntimeDeps{}
	}
	blockTree := fm.runtimeBlockTree()
	storedBlocks := fm.runtimeStoredBlocks()
	stagingStore := fm.runtimeStagingStore()
	return fetchscan.RuntimeDeps{
		StartHeight:  fm.scanConfig.StartHeight,
		Irreversible: fm.irreversibleBlocks,
		BlockTree:    blockTree,
		TaskPool:     fm.runtimeTaskPool(),
		StoreWorker:  fm.runtimeStoreWorker(),
		StoredBlocks: storedBlocks,
		ScanWorker:   fm.runtimeScanWorker(),
		StagingStore: stagingStore,
		NodeManager:  fm.nodeManager,
		Fetcher:      fm.fetcher,
		PruneRuntime: fetchscan.PruneRuntimeDeps{
			BlockTree:    fm.runtimeBlockTree(),
			StagingStore: stagingStore,
			StoredBlocks: storedBlocks,
			TaskPool:     fm.runtimeTaskPool(),
		},
	}
}

func (fm *FetchManager) Run(ctx context.Context) {
	if fm == nil {
		return
	}
	if fm.election == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	go fm.election.DoWithLeaderElection(ctx, "scanEvents", time.Second, fm.onBecameLeader, fm.onLostLeader)
}

func (fm *FetchManager) Stop() {
	if fm == nil {
		return
	}
	fm.releaseLeaderRuntime(true)
	if fm.election != nil {
		fm.election.TriggerLostLeader()
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
		if storeWorker == nil {
			return map[string]any{}
		}
		return storeWorker.MetricsPayload()
	}))
}

func (fm *FetchManager) onBecameLeader(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	fm.createRuntimeState()

	if fm.dbOperator == nil {
		fm.releaseLeaderRuntime(true)
		return fmt.Errorf("db operator is nil")
	}
	blocks, err := fm.dbOperator.LoadBlockWindowFromDB(ctx)
	if err != nil {
		fm.releaseLeaderRuntime(true)
		return err
	}

	loaded, err := fm.restoreBlockTree(blocks)
	if err != nil {
		logrus.Errorf("leader bootstrap load latest blocks from db failed: %v", err)
		fm.releaseLeaderRuntime(true)
		return fmt.Errorf("leader bootstrap load latest blocks from db failed: %w", err)
	}

	if loaded > 0 {
		logrus.Infof("leader bootstrap from db success. loaded:%v", loaded)
	} else {
		if !fm.runtimeScanFlow().EnsureBootstrapHeader() {
			logrus.Errorf("leader bootstrap from remote failed")
			fm.releaseLeaderRuntime(true)
			return fmt.Errorf("leader bootstrap from remote failed")
		}

		logrus.Infof("leader bootstrap from remote success")
	}

	fm.startLeaderRuntime(ctx)

	return nil
}

func (fm *FetchManager) onLostLeader(_ context.Context) error {
	fm.releaseLeaderRuntime(true)
	logrus.Infof("leader runtime state released")
	return nil
}

func (fm *FetchManager) startLeaderRuntime(ctx context.Context) {
	if fm == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rt := fm.currentRuntime()
	if rt == nil {
		return
	}
	rt.headerManager.Start(ctx)
	rt.taskPool.Start()
	rt.scanWorker.SetEnabled(true)
	rt.scanWorker.Start()
	fm.startRedisMessagePushLoop()
}

func (fm *FetchManager) releaseLeaderRuntime(dropState bool) {
	if fm == nil {
		return
	}
	rt := fm.currentRuntime()
	if rt != nil {
		rt.scanWorker.SetEnabled(false)
	}
	fm.stopRuntimeWorkers()
	fm.stopRedisMessagePushLoop()
	if rt != nil {
		rt.taskPool.Stop()
	}
	if dropState {
		fm.deleteRuntimeState()
	}
}
