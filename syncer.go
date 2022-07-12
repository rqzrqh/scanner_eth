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
	ds           *fetch.DataSource
	fm           *fetch.FetchManager
	sm           *store.StoreManager
	fetchWorkers []*fetch.FetchWorker
	storeWorkers []*store.StoreWorker
	event_center *event.EventCenter
}

func newSyncer(client *rpc.Client, db *gorm.DB, storeChannelSize int, storeBatchSize int, storeWorkerCount int, fetchTaskWindowSize int, fetchWorkerCount int, startHeight uint64, endHeight uint64) *Syncer {

	logrus.Infof("storeChannelSize:%v storeBatchSize:%v storeWorkerCount:%v fetchTaskWindowSize:%v fetchWorkerCount:%v startHeight:%v endHeight:%v",
		storeChannelSize, storeBatchSize, storeWorkerCount, fetchTaskWindowSize, fetchWorkerCount, startHeight, endHeight)

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
	blkJson := &types.BlockJson{}

	if err := client.Call(blkJson, "eth_getBlockByNumber", util.ToBlockNumArg(blockNum), false); err != nil {
		logrus.Warnf("startup failed to get latest block. height:%v err:%v", blockNum.String(), err)
		os.Exit(0)
	}

	if blkJson == nil || blkJson.Number == "" {
		logrus.Warnf("startup get empty block")
		os.Exit(0)
	}

	startBlockHeight := hexutil.MustDecodeUint64(blkJson.Number)
	startBlockHash := blkJson.Hash
	startBlockParentHash := blkJson.ParentHash

	//startBlockHeight, startBlockHash, startBlockParentHash := loadStartBlock(client, db, startHeight, storeBatchSize, storeTaskChannel, storeCompleteChannel)
	endBlockHeight, endBlockHash := loadEndBlock(client, db, endHeight)

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
	mc := fetch.NewMemoryChain(reversibleSize, blkDigestList)

	ds := fetch.NewDataSource(client, endBlockHeight, endBlockHash)
	differ := fetch.NewDiffer(mc, ds.GetRemoteChain())

	fm := fetch.NewFetchManager(differ, mc, sm, fetchTaskWindowSize, storeEventChannel)

	fetchWorkers := make([]*fetch.FetchWorker, 0)
	for i := 0; i < fetchWorkerCount; i++ {
		fetchWorkers = append(fetchWorkers, fetch.NewFetchWorker(i, fm, client))
	}

	event_center := event.NewEventCenter(publishEventChannel)

	return &Syncer{
		ds:           ds,
		sm:           sm,
		fm:           fm,
		fetchWorkers: fetchWorkers,
		storeWorkers: storeWorkers,
		event_center: event_center,
	}
}

func (s *Syncer) Run() {
	s.ds.Run()
	s.fm.Run()
	s.sm.Run()

	s.event_center.Run()

	for _, w := range s.fetchWorkers {
		w.Run()
	}

	// storeWorkers run before
}

func loadStartBlock(client *rpc.Client, db *gorm.DB, height uint64, batchSize int, storeTaskChannel chan *store.StoreTask, storeCompleteChannel chan *store.StoreComplete) (uint64, string, string) {
	return uint64(0), "", ""
}

func loadEndBlock(client *rpc.Client, db *gorm.DB, height uint64) (uint64, string) {
	if height != math.MaxUint64 {
		logrus.Infof("end block is fixed")

		fullblock := fetch.FetchFullBlock(-1, client, height)
		if fullblock == nil {
			logrus.Errorf("init get end block failed %v", height)
			os.Exit(0)
		}

		endBlockHeight := fullblock.Block.Height
		endBlockHash := fullblock.Block.BlockHash
		logrus.Infof("read end block height:%v hash:%v", endBlockHeight, endBlockHash)

		return endBlockHeight, endBlockHash
	}

	logrus.Infof("end block is dynamic")

	return height, ""
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
