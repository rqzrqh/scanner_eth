package fetch

import (
	"context"
	"encoding/json"
	"scanner_eth/leader"
	"scanner_eth/model"
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
	chainId                  int64
	forkVersion              uint64
	remoteChainUpdateChannel <-chan *types.RemoteChainUpdate
	hns                      []*HeaderNotifier
}

func NewFetchManager(
	chainName string,
	clients []*ethclient.Client,
	redisClient *redis.Client,
	interval time.Duration,
	executeAgain bool,
	localChain *LocalChain,
	endHeight uint64,
	maxUnorganizedBlockCount int,
	db *gorm.DB,
	chainId int64,
) *FetchManager {

	InitErc20Cache()
	InitErc721Cache()

	election := leader.NewElection(chainName, redisClient)

	remoteChainUpdateChannel := make(chan *types.RemoteChainUpdate, 100)
	hns := make([]*HeaderNotifier, len(clients))
	for i, client := range clients {
		hns[i] = NewHeaderNotifier(i, client, remoteChainUpdateChannel)
	}

	_ = endHeight
	_ = maxUnorganizedBlockCount

	return &FetchManager{
		election:                 election,
		interval:                 interval,
		executeAgain:             executeAgain,
		nodeManager:              NewNodeManager(clients),
		localChain:               localChain,
		eventHeightManager:       NewEventHeightManager(),
		db:                       db,
		chainId:                  chainId,
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
		item := fm.eventHeightManager.GetOrCreate(h)

		if item.State == EventHeightStateNoData {
			nodeId, client, err := fm.nodeManager.GetBestNode(h)
			if err != nil {
				return true, nil
			}

			fm.eventHeightManager.SetState(h, EventHeightStateSyncing)
			height := h
			syncTaskId := item.SyncTaskId
			go func() {
				startTime := time.Now()
				fullBlock := FetchFullBlock(nodeId, syncTaskId, client, fm.db, height)
				costTime := time.Since(startTime)
				var blockData *EventBlockData
				if fullBlock != nil {
					blockData = &EventBlockData{
						Height:            fullBlock.Block.Height,
						Hash:              fullBlock.Block.Hash,
						ParentHash:        fullBlock.Block.ParentHash,
						StorageFullBlock:  ConvertStorageFullBlock(fullBlock),
						ProtocolFullBlock: ConvertProtocolFullBlock(fullBlock),
					}
				}
				fm.eventHeightManager.SetResult(height, blockData)
				fm.nodeManager.UpdateNodeState(nodeId, costTime.Microseconds(), fullBlock != nil)
			}()
			continue
		} else if item.State == EventHeightStateWriting {
			return false, nil
		} else if item.State == EventHeightStateHasData {

		}

		if item.BlockData == nil {
			continue
		}
		bd := item.BlockData
		currentHeight, tipHash := fm.localChain.GetChainInfo()

		if bd.ParentHash != tipHash {
			fm.forkVersion++
			logrus.Warnf("scanEvents parent hash mismatch height:%v tip:%v parent:%v", h, tipHash, bd.ParentHash)
			if currentHeight > 0 {
				fm.localChain.Revert(currentHeight)
				var si model.ScannerInfo
				if err := fm.db.Where("chain_id = ?", fm.chainId).First(&si).Error; err != nil {
					logrus.Errorf("scanEvents revert get scanner_info failed: %v", err)
				} else {
					rb := buildChainBinlogRevert(currentHeight)
					rb.ChainId = fm.chainId
					rb.MessageId = si.MessageId + 1
					bd, _ := json.Marshal(rb)
					if _, err := Revert(fm.db, currentHeight, rb, bd); err != nil {
						logrus.Errorf("scanEvents db revert failed: %v", err)
					}
				}
			}
			continue
		}

		var si model.ScannerInfo
		if err := fm.db.Where("chain_id = ?", fm.chainId).First(&si).Error; err != nil {
			logrus.Errorf("scanEvents get scanner_info failed: %v", err)
			return true, nil
		}

		chainBinlog := buildChainBinlogApply(bd.Height, bd.ProtocolFullBlock)
		if chainBinlog == nil {
			continue
		}
		chainBinlog.ChainId = fm.chainId
		chainBinlog.MessageId = si.MessageId + 1
		binlogData, err := json.Marshal(chainBinlog)
		if err != nil {
			logrus.Errorf("scanEvents marshal chain binlog failed: %v", err)
			continue
		}

		if bd.StorageFullBlock == nil {
			continue
		}
		var doneBlk model.Block
		if err := fm.db.Where("height = ? AND complete = ?", bd.Height, true).First(&doneBlk).Error; err == nil {
			if cur, _ := fm.localChain.GetChainInfo(); cur < bd.Height {
				if err := fm.localChain.Grow(bd.Height, bd.Hash, bd.ParentHash); err != nil {
					rb := buildChainBinlogRevert(cur)
					rb.ChainId = fm.chainId
					rb.MessageId = si.MessageId + 1
					bd, _ := json.Marshal(rb)
					if _, err := Revert(fm.db, cur, rb, bd); err != nil {
						logrus.Errorf("scanEvents db revert failed: %v", err)
					}
					fm.localChain.Revert(cur)
				}
			}
			fm.eventHeightManager.SetState(h, EventHeightStateWriting)
			continue
		}

		if err := fm.localChain.Grow(bd.Height, bd.Hash, bd.ParentHash); err != nil {
			rb := buildChainBinlogRevert(currentHeight)
			rb.ChainId = fm.chainId
			rb.MessageId = si.MessageId + 1
			bd, _ := json.Marshal(rb)
			if _, err := Revert(fm.db, currentHeight, rb, bd); err != nil {
				logrus.Errorf("scanEvents db revert failed: %v", err)
			}
			fm.localChain.Revert(currentHeight)
			continue
		}
		fm.eventHeightManager.SetState(h, EventHeightStateWriting)
		_, err = StoreFullBlock(fm.db, bd.StorageFullBlock, chainBinlog, binlogData)
		if err != nil {
			logrus.Errorf("scanEvents StoreFullBlock failed height:%v err:%v", h, err)
			fm.localChain.Revert(bd.Height)
			continue
		}
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
			Height:            fullBlock.Block.Height,
			Hash:              fullBlock.Block.Hash,
			ParentHash:        fullBlock.Block.ParentHash,
			StorageFullBlock:  ConvertStorageFullBlock(fullBlock),
			ProtocolFullBlock: ConvertProtocolFullBlock(fullBlock),
		}
	}
	fm.eventHeightManager.SetResult(height, blockData)
	fm.nodeManager.UpdateNodeState(nodeId, costTime.Microseconds(), fullBlock != nil)
}
