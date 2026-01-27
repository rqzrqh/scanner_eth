package main

import (
	"math"
	"math/big"
	"os"
	"sync_eth/event"
	"sync_eth/fetch"
	"sync_eth/model"
	"sync_eth/store"
	"sync_eth/types"
	"sync_eth/util"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type Syncer struct {
	hns          []*fetch.HeaderNotifier
	fm           *fetch.FetchManager
	sm           *store.StoreManager
	storeWorkers []*store.StoreWorker
	event_center *event.EventCenter
}

func newSyncer(clients []*rpc.Client, db *gorm.DB, reversibleBlocks int, storeChannelSize int, storeBatchSize int, storeWorkerCount int, startHeight uint64, endHeight uint64) *Syncer {

	logrus.Infof("reversibleBlocks:%v storeChannelSize:%v storeBatchSize:%v storeWorkerCount:%v startHeight:%v endHeight:%v",
		reversibleBlocks, storeChannelSize, storeBatchSize, storeWorkerCount, startHeight, endHeight)

	fetch.InitAbi()

	blkDigestList := loadStartBlock(clients, db, startHeight, reversibleBlocks)

	localChain := fetch.NewLocalChain(reversibleBlocks, blkDigestList)

	storeOperationChannel := make(chan *types.StoreOperation, storeChannelSize)

	storeTaskChannel := make(chan *store.StoreTask, 10)
	storeCompleteChannel := make(chan *store.StoreComplete, 10)

	storeWorkers := make([]*store.StoreWorker, storeWorkerCount)
	for i := 0; i < storeWorkerCount; i++ {
		worker := store.NewStoreWorker(i, db, storeTaskChannel, storeCompleteChannel)
		storeWorkers[i] = worker
	}

	sm := store.NewStoreManager(db, storeBatchSize, storeOperationChannel, storeTaskChannel, storeCompleteChannel)

	remoteChainUpdateChannel := make(chan *types.RemoteChainUpdate, 100)

	hns := make([]*fetch.HeaderNotifier, len(clients))
	for i, client := range clients {
		hns[i] = fetch.NewHeaderNotifier(i, client, remoteChainUpdateChannel, endHeight)
	}

	maxUnorganizedBlockCount := 50 * len(clients)

	publishOperationChannel := make(chan *types.PublishOperation, 0)
	fm := fetch.NewFetchManager(clients, localChain, maxUnorganizedBlockCount, remoteChainUpdateChannel, storeOperationChannel, publishOperationChannel)
	event_center := event.NewEventCenter(publishOperationChannel)

	return &Syncer{
		hns:          hns,
		sm:           sm,
		fm:           fm,
		storeWorkers: storeWorkers,
		event_center: event_center,
	}
}

func (s *Syncer) Run() {
	for _, hn := range s.hns {
		hn.Run()
	}

	for _, sw := range s.storeWorkers {
		sw.Run()
	}

	s.fm.Run()
	s.sm.Run()

	s.event_center.Run()
}

func loadStartBlock(clients []*rpc.Client, db *gorm.DB, startHeight uint64, reversibleBlocks int) []*fetch.BlockDigest {
	blkDigestList := make([]*fetch.BlockDigest, 0)

	if startHeight == math.MaxUint64 {
		var latestBlock model.Block
		if err := db.Select("height").Order("height desc").Limit(1).Find(&latestBlock).Error; err != nil {
			logrus.Errorf("failed to get max block height from db %v", err)
			os.Exit(0)
		}

		logrus.Infof("startup load lookback blocks from db. height:%v", latestBlock.Height)

		lookbackBlockList := lookbackBlock(db, latestBlock.Height, reversibleBlocks)
		for _, v := range lookbackBlockList {
			blk := &fetch.BlockDigest{
				Height:     v.Height,
				Hash:       v.BlockHash,
				ParentHash: v.ParentHash,
			}
			blkDigestList = append(blkDigestList, blk)
		}
	} else {

		blockNum := new(big.Int).SetUint64(startHeight - 1)
		blkJson := &types.BlockHeaderJson{}

		logrus.Infof("startup load latest block from rpc. height:%v", blockNum.Uint64())

		if err := clients[0].Call(blkJson, "eth_getBlockByNumber", util.ToBlockNumArg(blockNum), false); err != nil {
			logrus.Warnf("startup failed to get specific block. height:%v err:%v", blockNum.String(), err)
			os.Exit(0)
		}

		if blkJson.Number == "" {
			logrus.Warnf("startup get empty block. height:%v", blockNum.Uint64())
			os.Exit(0)
		}

		startBlockHeight := hexutil.MustDecodeUint64(blkJson.Number)
		startBlockHash := blkJson.Hash
		startBlockParentHash := blkJson.ParentHash

		blk := &fetch.BlockDigest{
			Height:     startBlockHeight,
			Hash:       startBlockHash,
			ParentHash: startBlockParentHash,
		}
		blkDigestList = append(blkDigestList, blk)
	}
	return blkDigestList
}

func lookbackBlock(db *gorm.DB, height uint64, reversibleBlocks int) []*model.Block {
	var modelBlockList []*model.Block
	var heightList []uint64

	for i := uint64(0); i <= uint64(reversibleBlocks); i++ {
		h := height - i
		if h > height {
			break
		}
		heightList = append(heightList, h)
	}

	logrus.Infof("lookback block heights: %v", heightList)

	if err := db.Where("height in ?", heightList).Order("height asc").Find(&modelBlockList).Error; err != nil {
		logrus.Errorf("lookback block failed: %v", err)
		os.Exit(0)
	}

	return modelBlockList
}
