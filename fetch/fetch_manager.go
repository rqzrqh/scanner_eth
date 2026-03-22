package fetch

import (
	"context"
	"expvar"
	"fmt"
	"scanner_eth/blocktree"
	"scanner_eth/leader"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type FetchManager struct {
	election                 *leader.Election
	scanLoopCancel           context.CancelFunc
	scanTriggerCh            chan struct{}
	scanEnabled              atomic.Bool
	nodeManager              *NodeManager
	blockTree                *blocktree.BlockTree
	db                       *gorm.DB
	chainId                  int64
	startHeight              uint64
	irreversibleBlocks       int
	storedBlocks             storedBlockState
	pendingPayloadStore      *BlockPayloadStore
	taskPool                 taskPool
	remoteChainUpdateChannel <-chan *RemoteChainUpdate
	hns                      []*HeaderNotifier
	taskPoolOptions          TaskPoolOptions

	blockFetcher BlockFetcher
	dbOperator   DbOperator
}

func NewFetchManager(
	chainName string,
	clients []*ethclient.Client,
	redisClient *redis.Client,
	startHeight uint64,
	endHeight uint64,
	reversibleBlocks int,
	taskPoolOptions TaskPoolOptions,
	db *gorm.DB,
	chainId int64,
	dbOperator DbOperator,
	blockFetcher BlockFetcher,
) *FetchManager {
	InitErc20Cache()
	InitErc721Cache()

	election := leader.NewElection(chainName, redisClient)

	remoteChainUpdateChannel := make(chan *RemoteChainUpdate, 100)
	hns := make([]*HeaderNotifier, len(clients))
	for i, client := range clients {
		hns[i] = NewHeaderNotifier(i, client, remoteChainUpdateChannel)
	}

	_ = endHeight
	taskPoolOptions = normalizeTaskPoolOptions(taskPoolOptions, len(clients))

	fm := &FetchManager{
		election:                 election,
		scanTriggerCh:            make(chan struct{}, 1),
		nodeManager:              NewNodeManager(clients),
		db:                       db,
		chainId:                  chainId,
		startHeight:              startHeight,
		irreversibleBlocks:       reversibleBlocks,
		remoteChainUpdateChannel: remoteChainUpdateChannel,
		hns:                      hns,
		taskPoolOptions:          taskPoolOptions,
		dbOperator:               dbOperator,
		blockFetcher:             blockFetcher,
	}
	fm.createRuntimeState()
	return fm
}

func (fm *FetchManager) Run() {
	if fm == nil {
		return
	}
	fm.startHeaderNotifiersAndConsumer()
	if fm.election == nil {
		return
	}
	go fm.election.DoWithLeaderElection(context.Background(), "scanEvents", time.Second, fm.onBecameLeader, fm.onLostLeader)
}

func (fm *FetchManager) Stop() {
	if fm == nil {
		return
	}
	fm.stopScanLoop()
	if fm.election != nil {
		fm.election.TriggerLostLeader()
	}
	fm.taskPool.stop()
}

func (fm *FetchManager) EnableTaskPoolMetrics(name string) {
	if strings.TrimSpace(name) == "" {
		name = "fetch_task_pool"
	}
	fm.taskPool.metricsOnce.Do(func() {
		expvar.Publish(name, expvar.Func(func() any {
			return fm.taskPool.metricsPayload()
		}))
	})
}

func (fm *FetchManager) startHeaderNotifiersAndConsumer() {
	for _, hn := range fm.hns {
		hn.Run()
	}
	go func() {
		for remoteChainUpdate := range fm.remoteChainUpdateChannel {
			if remoteChainUpdate == nil {
				continue
			}
			fm.nodeManager.UpdateNodeChainInfo(remoteChainUpdate.NodeId, remoteChainUpdate.Height, remoteChainUpdate.BlockHash)
			if remoteChainUpdate.Header != nil && fm.isScanEnabled() {
				fm.insertHeader(&BlockHeaderJson{
					Hash:       remoteChainUpdate.Header.Hash,
					ParentHash: remoteChainUpdate.Header.ParentHash,
					Number:     remoteChainUpdate.Header.Number,
					Difficulty: remoteChainUpdate.Header.Difficulty,
				})
			}
			fm.triggerScan()
		}
	}()
}

func (fm *FetchManager) onBecameLeader(ctx context.Context) error {
	_ = ctx
	fm.createRuntimeState()

	blocks, err := fm.dbOperator.LoadBlockWindowFromDB()
	if err != nil {
		return err
	}

	loaded, err := fm.restoreBlockTree(blocks)
	if err != nil {
		return fmt.Errorf("leader bootstrap load latest blocks from db failed: %w", err)
	}

	if loaded > 0 {
		logrus.Infof("leader bootstrap from db success. loaded:%v", loaded)
	} else {
		if !fm.ensureBootstrapHeader() {
			return fmt.Errorf("leader bootstrap from remote failed")
		}

		logrus.Infof("leader bootstrap from remote success")
	}

	fm.taskPool.start()
	fm.scanEnabled.Store(true)
	fm.stopScanLoop()
	fm.startScanLoop()

	return nil
}

func (fm *FetchManager) onLostLeader(ctx context.Context) error {
	_ = ctx
	fm.scanEnabled.Store(false)
	fm.stopScanLoop()
	fm.taskPool.stop()
	fm.deleteRuntimeState()
	logrus.Infof("leader runtime state released")
	return nil
}

func (fm *FetchManager) startScanLoop() {

	loopCtx, cancel := context.WithCancel(context.Background())
	fm.scanLoopCancel = cancel

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				fm.scanEvents(loopCtx)
			case <-fm.scanTriggerCh:
				fm.scanEvents(loopCtx)
			}
		}
	}()
}

func (fm *FetchManager) triggerScan() {
	if fm == nil || fm.scanTriggerCh == nil || !fm.isScanEnabled() {
		return
	}
	select {
	case fm.scanTriggerCh <- struct{}{}:
	default:
	}
}

func (fm *FetchManager) isScanEnabled() bool {
	if fm == nil {
		return false
	}
	return fm.scanEnabled.Load()
}

func (fm *FetchManager) stopScanLoop() {
	if fm.scanLoopCancel != nil {
		fm.scanLoopCancel()
		fm.scanLoopCancel = nil
	}
}

func (fm *FetchManager) createRuntimeState() {
	fm.nodeManager.SetAllNodesIdle()
	fm.storedBlocks = newStoredBlockState()
	fm.pendingPayloadStore = NewBlockPayloadStore()
	fm.blockTree = blocktree.NewBlockTree(fm.irreversibleBlocks)
	fm.taskPool = newTaskPoolWithStop(fm.taskPoolOptions, fm.nodeManager.NodeCount(), fm.handleTaskPoolTask)
}

func (fm *FetchManager) deleteRuntimeState() {
	fm.storedBlocks = storedBlockState{}
	fm.pendingPayloadStore = nil
	fm.blockTree = nil
	fm.taskPool = taskPool{}
}

func (fm *FetchManager) setNodeBlockHeader(hash string, header *BlockHeaderJson) bool {
	if fm.blockTree == nil || fm.pendingPayloadStore == nil {
		return false
	}
	node := fm.blockTree.Get(hash)
	if node == nil {
		return false
	}
	fm.pendingPayloadStore.SetBlockHeader(hash, header)
	return true
}

func (fm *FetchManager) setNodeBlockBody(hash string, data *EventBlockData) bool {
	if fm.blockTree == nil || fm.pendingPayloadStore == nil {
		return false
	}
	node := fm.blockTree.Get(hash)
	if node == nil {
		return false
	}
	fm.pendingPayloadStore.SetBlockBody(hash, data)
	return true
}

func (fm *FetchManager) getNodeBlockHeader(hash string) *BlockHeaderJson {
	if fm.pendingPayloadStore == nil {
		return nil
	}
	return fm.pendingPayloadStore.GetBlockHeader(hash)
}

func (fm *FetchManager) getNodeBlockBody(hash string) *EventBlockData {
	if fm.pendingPayloadStore == nil {
		return nil
	}
	return fm.pendingPayloadStore.GetBlockBody(hash)
}

func (fm *FetchManager) deleteNodeBlockPayload(hash string) {
	if fm.pendingPayloadStore == nil {
		return
	}
	fm.pendingPayloadStore.DeleteBlockPayload(hash)
}
