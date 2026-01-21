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

var reversibleSize = 27

type Syncer struct {
	dses         []*fetch.DataSource
	fm           *fetch.FetchManager
	sm           *store.StoreManager
	storeWorkers []*store.StoreWorker
	event_center *event.EventCenter
}

func newSyncer(clients []*rpc.Client, db *gorm.DB, storeChannelSize int, storeBatchSize int, storeWorkerCount int, startHeight uint64, endHeight uint64) *Syncer {

	logrus.Infof("storeChannelSize:%v storeBatchSize:%v storeWorkerCount:%v startHeight:%v endHeight:%v",
		storeChannelSize, storeBatchSize, storeWorkerCount, startHeight, endHeight)

	fetch.InitAbi()

	storeTaskChannel := make(chan *store.StoreTask, 10)
	storeCompleteChannel := make(chan *store.StoreComplete, 10)

	storeWorkers := make([]*store.StoreWorker, 0)
	for i := 0; i < storeWorkerCount; i++ {
		worker := store.NewStoreWorker(i, db, storeTaskChannel, storeCompleteChannel)
		worker.Run()
		storeWorkers = append(storeWorkers, worker)
	}

	// TODO get genesis block

	if startHeight != math.MaxUint64 && endHeight != math.MaxUint64 {
		if startHeight > endHeight {
			logrus.Errorf("start height and end height error %v %v", startHeight, endHeight)
			os.Exit(0)
		}
	}

	blockNum := new(big.Int).SetUint64(startHeight)
	blkJson := &types.BlockHeaderJson{}

	if err := clients[0].Call(blkJson, "eth_getBlockByNumber", util.ToBlockNumArg(blockNum), false); err != nil {
		logrus.Warnf("startup failed to get latest block. height:%v err:%v", blockNum.String(), err)
		os.Exit(0)
	}

	if blkJson.Number == "" {
		logrus.Warnf("startup get empty block height:%v", startHeight)
		os.Exit(0)
	}

	startBlockHeight := hexutil.MustDecodeUint64(blkJson.Number)
	startBlockHash := blkJson.Hash
	startBlockParentHash := blkJson.ParentHash

	//startBlockHeight, startBlockHash, startBlockParentHash := loadStartBlock(client, db, startHeight, storeBatchSize, storeTaskChannel, storeCompleteChannel)

	blkDigestList := make([]*fetch.BlockDigest, 0)
	if startHeight == math.MaxUint64 {
		lookbackBlockList := lookbackBlock(db, startBlockHeight, reversibleSize)
		for _, v := range lookbackBlockList {
			blk := &fetch.BlockDigest{
				Height:     v.Height,
				Hash:       v.BlockHash,
				ParentHash: v.ParentHash,
			}
			blkDigestList = append(blkDigestList, blk)
		}
	} else {
		// use start block
		blk := &fetch.BlockDigest{
			Height:     startBlockHeight,
			Hash:       startBlockHash,
			ParentHash: startBlockParentHash,
		}
		blkDigestList = append(blkDigestList, blk)
	}

	storeEventChannel := make(chan *types.ChainEvent, storeChannelSize)
	publishEventChannel := make(chan *types.ChainEvent, 0)

	sm := store.NewStoreManager(db, storeBatchSize, storeEventChannel, publishEventChannel, storeTaskChannel, storeCompleteChannel)
	localChain := fetch.NewLocalChain(reversibleSize, blkDigestList)

	remoteChainUpdateChannel := make(chan *types.RemoteChainUpdate, 100)

	dses := make([]*fetch.DataSource, len(clients))
	for i, client := range clients {
		dses[i] = fetch.NewDataSource(i, client, remoteChainUpdateChannel)
	}

	maxUnorganizedBlockCount := 50 * len(clients)

	fm := fetch.NewFetchManager(clients, localChain, maxUnorganizedBlockCount, remoteChainUpdateChannel, storeEventChannel)

	event_center := event.NewEventCenter(publishEventChannel)

	return &Syncer{
		dses:         dses,
		sm:           sm,
		fm:           fm,
		storeWorkers: storeWorkers,
		event_center: event_center,
	}
}

func (s *Syncer) Run() {
	for _, ds := range s.dses {
		ds.Run()
	}

	s.fm.Run()
	s.sm.Run()

	s.event_center.Run()

	// storeWorkers run before
}

func loadStartBlock(client *rpc.Client, db *gorm.DB, height uint64, batchSize int, storeTaskChannel chan *store.StoreTask, storeCompleteChannel chan *store.StoreComplete) (uint64, string, string) {
	return uint64(0), "", ""
}

func lookbackBlock(db *gorm.DB, height uint64, reversibleSize int) []*model.Block {
	var modelBlockList []*model.Block
	var heightList []uint64

	for i := uint64(0); i <= uint64(reversibleSize); i++ {
		h := height - i
		if h < 0 {
			break
		}
		heightList = append(heightList, 0)
	}

	if err := db.Where("height in ?", heightList).Order("height asc").Find(&modelBlockList).Error; err != nil {
		logrus.Errorf("lookback block failed")
		os.Exit(0)
	}

	return modelBlockList
}
