package fetch

import (
	"context"
	"encoding/json"
	"scanner_eth/fetch"
	"scanner_eth/leader"
	"scanner_eth/model"
	"scanner_eth/protocol"
	"scanner_eth/store"
	"scanner_eth/types"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type FetchManager struct {
	election                 *leader.Election
	interval                 time.Duration
	executeAgain             bool
	nodeManager              *NodeManager
	localChain               *LocalChain
	eventHeightManager       *EventHeightManager
	db                       *gorm.DB
	forkVersion              uint64
	remoteChainUpdateChannel <-chan *types.RemoteChainUpdate
	hns                      []*fetch.HeaderNotifier
}

func NewFetchManager(chainName string, clients []*ethclient.Client, redisClient *redis.Client, interval time.Duration, executeAgain bool, localChain *LocalChain, endHeight uint64, maxUnorganizedBlockCount int, db *gorm.DB) *FetchManager {

	InitErc20Cache()
	InitErc721Cache()

	election := leader.NewElection(chainName, redisClient)

	remoteChainUpdateChannel := make(chan *types.RemoteChainUpdate, 100)
	hns := make([]*fetch.HeaderNotifier, len(clients))
	for i, client := range clients {
		hns[i] = fetch.NewHeaderNotifier(i, client, remoteChainUpdateChannel)
	}

	return &FetchManager{
		election:                 election,
		interval:                 interval,
		executeAgain:             executeAgain,
		nodeManager:              NewNodeManager(clients),
		localChain:               localChain,
		eventHeightManager:       NewEventHeightManager(),
		db:                       db,
		forkVersion:              0,
		remoteChainUpdateChannel: remoteChainUpdateChannel,
		hns:                      hns,
	}
}

func (fm *FetchManager) Run() {

	for _, hn := range fm.hns {
		hn.Run()
	}
	go fm.election.DoWithLeaderElection(context.Background(), "scanEvents", fm.interval, fm.executeAgain, fm.scanEvents)

	go func() {
		for {
			select {
			case remoteChainUpdate := <-fm.remoteChainUpdateChannel:
				fm.nodeManager.UpdateNodeChainInfo(remoteChainUpdate.NodeId, remoteChainUpdate.Height, remoteChainUpdate.BlockHash)
			}
		}
	}()
}

func (fm *FetchManager) scanEvents(ctx context.Context) (bool, error) {
	var latestHeight uint64
	if err := fm.db.Model(&model.Block{}).Order("height desc").Limit(1).Pluck("height", &latestHeight).Error; err != nil {
		logrus.Warnf("scanEvents get latest height from db failed: %v", err)
		return true, nil
	}
	if latestHeight == 0 {
		return true, nil
	}

	rangeSize := 2 * fm.nodeManager.NodeCount()
	var startHeight uint64 = 1
	if latestHeight > uint64(rangeSize) {
		startHeight = latestHeight - uint64(rangeSize) + 1
	}

	for h := startHeight; h <= latestHeight; h++ {
		item, exists := fm.eventHeightManager.Get(h)

		// （1）不存在：新建，获取节点失败则返回；成功则起协程同步，状态设为同步中，continue
		if !exists {
			newItem, created := fm.eventHeightManager.GetOrCreate(h)
			if !created {
				continue
			}
			nodeId, client, err := fm.nodeManager.GetBestNode(h)
			if err != nil {
				return true, nil
			}
			fm.eventHeightManager.SetState(h, EventHeightStateSyncing)
			height := h
			syncTaskId := newItem.SyncTaskId
			go func() {
				startTime := time.Now()
				fullBlock := FetchFullBlock(nodeId, syncTaskId, client, fm.db, height)
				costTime := time.Since(startTime)
				var blockData *EventBlockData
				if fullBlock != nil {
					blockData = &EventBlockData{
						FullBlock:         fullBlock,
						StorageFullBlock:  store.ConvertStorageFullBlock(fullBlock),
						ProtocolFullBlock: store.ConvertProtocolFullBlock(fullBlock),
					}
				}
				fm.eventHeightManager.SetResult(height, blockData)
				fm.nodeManager.UpdateNodeState(nodeId, costTime.Microseconds(), fullBlock != nil)
			}()
			continue
		}

		// （2）没有数据：获取节点失败则返回；成功则 continue
		if item.State == EventHeightStateSyncing || item.State == EventHeightStateNoData {
			_, _, err := fm.nodeManager.GetBestNode(h)
			if err != nil {
				return true, nil
			}
			continue
		}

		// （3）有数据：是否可衔接由 Grow 决定；不能衔接则回滚，能衔接则写库，状态设为写入中
		if item.BlockData == nil || item.BlockData.FullBlock == nil {
			continue
		}
		blk := item.BlockData.FullBlock.Block
		currentHeight, _ := fm.localChain.GetChainInfo()

		if err := fm.localChain.Grow(blk.Height, blk.Hash, blk.ParentHash); err != nil {
			fm.forkVersion++
			// Grow 判定不能衔接，回滚当前链顶，构造 Rollback 的 ChainBinlog 并下发
			logrus.Warnf("scanEvents grow failed. height:%v err:%v", h, err)
			if currentHeight > 0 {
				fm.localChain.Revert(currentHeight)
				if fm.storeOperationChannel != nil {
					chainBinlog := buildChainBinlogRevert(currentHeight)
					fm.storeOperationChannel <- &types.StoreOperation{
						Type:        types.StoreRollback,
						Height:      currentHeight,
						ChainBinlog: chainBinlog,
					}
				}
			}
			continue
		}
		// Grow 成功，构造 Apply 的 ChainBinlog 并下发
		chainBinlog := buildChainBinlogApply(blk.Height, item.BlockData.ProtocolFullBlock)
		StoreFullBlock(sm.db, item.BlockData, chainBinlog, binlogData, sm.batchSize, sm.storeTaskChannel, sm.storeCompleteChannel)
		err != nil{}
		fm.eventHeightManager.SetState(h, EventHeightStateWriting)
	}
	return true, nil
}

func (fm *FetchManager) fetch(nodeId int, syncTaskId int, client *ethclient.Client, height uint64) {
	startTime := time.Now()
	fullBlock := FetchFullBlock(nodeId, syncTaskId, client, fm.db, height)
	costTime := time.Since(startTime)
	var blockData *EventBlockData
	if fullBlock != nil {
		blockData = &EventBlockData{
			FullBlock:         fullBlock,
			StorageFullBlock:  store.ConvertStorageFullBlock(fullBlock),
			ProtocolFullBlock: store.ConvertProtocolFullBlock(fullBlock),
		}
	}
	fm.eventHeightManager.SetResult(height, blockData)
	fm.nodeManager.UpdateNodeState(nodeId, costTime.Microseconds(), fullBlock != nil)
}

// buildChainBinlogApply 根据 Grow 构造 Apply 的 ChainBinlog，Data 为协议块序列化
func buildChainBinlogApply(height uint64, protocolFullBlock *protocol.FullBlock) *protocol.ChainBinlog {
	if protocolFullBlock == nil {
		return nil
	}
	data, err := json.Marshal(protocolFullBlock)
	if err != nil {
		logrus.Warnf("buildChainBinlogApply marshal protocol fullblock failed. height:%v err:%v", height, err)
		return nil
	}
	return &protocol.ChainBinlog{
		ActionType: protocol.ChainActionApply,
		Height:     height,
		Data:       data,
	}
}

// buildChainBinlogRevert 根据 Revert 构造 Rollback 的 ChainBinlog
func buildChainBinlogRevert(height uint64) *protocol.ChainBinlog {
	return &protocol.ChainBinlog{
		ActionType: protocol.ChainActionRollback,
		Height:     height,
		Data:       nil,
	}
}
