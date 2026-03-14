package fetch

import (
	"context"
	"expvar"
	"fmt"
	"scanner_eth/blocktree"
	"scanner_eth/leader"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type FetchManager struct {
	election               *leader.Election
	redisClient            *redis.Client
	scanLoopCancel         context.CancelFunc
	redisMessageLoopCancel context.CancelFunc
	scannerMsgRedisBatch   int
	scanTriggerCh          chan struct{}
	scanEnabled            atomic.Bool
	nodeManager            *NodeManager
	blockTree              *blocktree.BlockTree
	db                     *gorm.DB
	chainName              string
	chainId                int64
	startHeight            uint64
	irreversibleBlocks     int
	storedBlocks           storedBlockState
	pendingPayloadStore    *BlockPayloadStore
	taskPool               taskPool
	hns                    []*HeaderNotifier
	taskPoolOptions        TaskPoolOptions

	headerNotifyMu      sync.Mutex
	headerNotifyCancel  context.CancelFunc
	headerNotifierWg    sync.WaitGroup
	headerConsumerWg    sync.WaitGroup
	remoteChainUpdateCh chan *RemoteChainUpdate

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
	rpcTimeout time.Duration,
	taskPoolOptions TaskPoolOptions,
	db *gorm.DB,
	chainId int64,
	dbOperator DbOperator,
	blockFetcher BlockFetcher,
) *FetchManager {
	InitErc20Cache()
	InitErc721Cache()

	election := leader.NewElection(chainName, redisClient)

	hns := make([]*HeaderNotifier, len(clients))
	for i, client := range clients {
		hns[i] = NewHeaderNotifier(i, client)
	}

	_ = endHeight
	taskPoolOptions = normalizeTaskPoolOptions(taskPoolOptions, len(clients))

	fm := &FetchManager{
		election:             election,
		redisClient:          redisClient,
		scannerMsgRedisBatch: defaultScannerMessageRedisBatch,
		scanTriggerCh:        make(chan struct{}, 1),
		nodeManager:          NewNodeManager(clients, rpcTimeout),
		db:                   db,
		chainName:            chainName,
		chainId:              chainId,
		startHeight:          startHeight,
		irreversibleBlocks:   reversibleBlocks,
		hns:                  hns,
		taskPoolOptions:      taskPoolOptions,
		dbOperator:           dbOperator,
		blockFetcher:         blockFetcher,
	}
	fm.createRuntimeState()
	return fm
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
	fm.stopScanLoop()
	fm.stopRedisMessagePushLoop()
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

// startHeaderNotifiersAndConsumer allocates a channel and starts notifier workers plus the consumer.
// Caller must be leader (typically from onBecameLeader after bootstrap).
func (fm *FetchManager) startHeaderNotifiersAndConsumer(ctx context.Context) {
	ch := make(chan *RemoteChainUpdate, 100)
	fm.startHeaderNotifiersWithChannel(ctx, ch)
}

// startHeaderNotifiersWithChannel is used by tests to inject a custom channel; production uses startHeaderNotifiersAndConsumer.
func (fm *FetchManager) startHeaderNotifiersWithChannel(ctx context.Context, ch chan *RemoteChainUpdate) {
	if fm == nil || ch == nil {
		return
	}

	fm.headerNotifyMu.Lock()
	defer fm.headerNotifyMu.Unlock()

	fm.stopHeaderNotifiersLocked()

	notifyCtx, cancel := context.WithCancel(ctx)
	fm.headerNotifyCancel = cancel
	fm.remoteChainUpdateCh = ch

	fm.headerConsumerWg.Add(1)
	go func() {
		defer fm.headerConsumerWg.Done()
		for update := range ch {
			if update == nil {
				continue
			}
			fm.nodeManager.UpdateNodeChainInfo(update.NodeId, update.Height, update.BlockHash)
			if update.Header != nil && fm.isScanEnabled() {
				fm.insertHeader(&BlockHeaderJson{
					Hash:       update.Header.Hash,
					ParentHash: update.Header.ParentHash,
					Number:     update.Header.Number,
					Difficulty: update.Header.Difficulty,
				})
			}
			fm.triggerScan()
		}
	}()

	for _, hn := range fm.hns {
		if hn != nil {
			hn.Run(notifyCtx, ch, &fm.headerNotifierWg)
		}
	}
}

func (fm *FetchManager) stopHeaderNotifiersAndConsumer() {
	if fm == nil {
		return
	}
	fm.headerNotifyMu.Lock()
	defer fm.headerNotifyMu.Unlock()
	fm.stopHeaderNotifiersLocked()
}

// stopHeaderNotifiersLocked cancels notifier contexts, waits for notifier goroutines, closes the update channel, then waits for the consumer.
func (fm *FetchManager) stopHeaderNotifiersLocked() {
	if fm.headerNotifyCancel != nil {
		fm.headerNotifyCancel()
		fm.headerNotifyCancel = nil
	}
	fm.headerNotifierWg.Wait()
	if fm.remoteChainUpdateCh != nil {
		close(fm.remoteChainUpdateCh)
		fm.remoteChainUpdateCh = nil
	}
	fm.headerConsumerWg.Wait()
}

func (fm *FetchManager) onBecameLeader(ctx context.Context) error {
	fm.createRuntimeState()

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
		if !fm.ensureBootstrapHeader() {
			logrus.Errorf("leader bootstrap from remote failed")
			return fmt.Errorf("leader bootstrap from remote failed")
		}

		logrus.Infof("leader bootstrap from remote success")
	}

	fm.startHeaderNotifiersAndConsumer(context.Background())

	fm.taskPool.start()
	fm.scanEnabled.Store(true)
	fm.stopScanLoop()
	fm.startScanLoop()
	fm.startRedisMessagePushLoop()

	return nil
}

func (fm *FetchManager) onLostLeader(ctx context.Context) error {
	_ = ctx
	fm.scanEnabled.Store(false)
	fm.stopScanLoop()
	fm.stopRedisMessagePushLoop()
	fm.stopHeaderNotifiersAndConsumer()
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
	fm.nodeManager.ResetRemoteChainTips()
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
