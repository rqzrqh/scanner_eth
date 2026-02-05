package main

import (
	"math"
	"os"
	"scanner_eth/fetch"
	"scanner_eth/model"
	"scanner_eth/publish"
	"scanner_eth/store"
	"scanner_eth/types"
	"sort"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type Syncer struct {
	chainId          int64
	genesisBlockHash string
	hns              []*fetch.HeaderNotifier
	fm               *fetch.FetchManager
	sm               *store.StoreManager
	pm               *publish.PublishManager
}

func newSyncer(clients []*rpc.Client, db *gorm.DB, w *kafka.Writer, reversibleBlocks int, storeChannelSize int, storeBatchSize int, storeWorkerCount int, startHeight uint64, endHeight uint64, chainId int64, genesisBlockHash string, messageId uint64) *Syncer {

	logrus.Infof("reversibleBlocks:%v storeChannelSize:%v storeBatchSize:%v storeWorkerCount:%v startHeight:%v endHeight:%v",
		reversibleBlocks, storeChannelSize, storeBatchSize, storeWorkerCount, startHeight, endHeight)

	if startHeight > endHeight {
		logrus.Errorf("start height must be less than end height. startHeight:%v endHeight:%v", startHeight, endHeight)
		os.Exit(0)
	}

	storeOperationChannel := make(chan *types.StoreOperation, storeChannelSize)
	publishFeedbackOperationChannel := make(chan *types.PublishFeedbackOperation, 100)

	publishOperationChannel := make(chan *types.PublishOperation, 100)
	pm := publish.NewPublishManager(w, publishOperationChannel, publishFeedbackOperationChannel)

	sm := store.NewStoreManager(db, chainId, messageId, storeBatchSize, storeWorkerCount, storeOperationChannel, publishFeedbackOperationChannel, publishOperationChannel)

	remoteChainUpdateChannel := make(chan *types.RemoteChainUpdate, 100)
	maxUnorganizedBlockCount := 50 * len(clients)
	blkDigestList := loadStartBlock(clients, db, startHeight, reversibleBlocks)
	localChain := fetch.NewLocalChain(reversibleBlocks, blkDigestList)
	fm := fetch.NewFetchManager(clients, localChain, endHeight, maxUnorganizedBlockCount, remoteChainUpdateChannel, storeOperationChannel)

	hns := make([]*fetch.HeaderNotifier, len(clients))
	for i, client := range clients {
		hns[i] = fetch.NewHeaderNotifier(i, client, remoteChainUpdateChannel)
	}

	return &Syncer{
		chainId:          chainId,
		genesisBlockHash: genesisBlockHash,
		hns:              hns,
		fm:               fm,
		sm:               sm,
		pm:               pm,
	}
}

func (s *Syncer) Run() {

	s.pm.Run()
	s.sm.Run()
	s.fm.Run()

	for _, hn := range s.hns {
		hn.Run(s.chainId, s.genesisBlockHash)
	}
}

func loadStartBlock(clients []*rpc.Client, db *gorm.DB, startHeight uint64, reversibleBlocks int) []*fetch.BlockDigest {
	blkDigestList := make([]*fetch.BlockDigest, 0)

	var lookbackBlockList []*model.Block
	if err := db.Select("height").Order("height desc").Limit(reversibleBlocks).Find(&lookbackBlockList).Error; err != nil {
		logrus.Errorf("failed to lookback blocks from db %v", err)
		os.Exit(0)
	}

	if len(lookbackBlockList) == 0 {

		fetchStartHeight := startHeight

		// meaning from beginning
		if startHeight == 0 || startHeight == math.MaxUint64 {
			fetchStartHeight = 1
		}

		fullblock := fetch.FetchFullBlock(0, -1, clients[0], fetchStartHeight-1)
		if fullblock == nil {
			os.Exit(0)
		}

		if startHeight == 0 || startHeight == math.MaxUint64 {
			block := &model.Block{
				Height:     0,
				BlockHash:  fullblock.Block.BlockHash,
				ParentHash: fullblock.Block.ParentHash,
			}

			if err := db.Create(block).Error; err != nil {
				logrus.Errorf("insert genesis block to db failed. err:%v", err)
				os.Exit(0)
			}
		}

		lookbackBlockList = append(lookbackBlockList, &model.Block{
			Height:     fullblock.Block.Height,
			BlockHash:  fullblock.Block.BlockHash,
			ParentHash: fullblock.Block.ParentHash,
		})

	} else {
		sort.Slice(lookbackBlockList, func(i, j int) bool {
			return lookbackBlockList[i].Height < lookbackBlockList[j].Height
		})
	}

	for _, v := range lookbackBlockList {
		blk := &fetch.BlockDigest{
			Height:     v.Height,
			Hash:       v.BlockHash,
			ParentHash: v.ParentHash,
		}
		blkDigestList = append(blkDigestList, blk)
	}

	return blkDigestList
}
