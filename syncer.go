package main

import (
	"math"
	"math/big"
	"os"
	"scanner_eth/fetch"
	"scanner_eth/model"
	"scanner_eth/publish"
	"scanner_eth/store"
	"scanner_eth/types"
	"scanner_eth/util"
	"sort"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type Syncer struct {
	hns []*fetch.HeaderNotifier
	fm  *fetch.FetchManager
	sm  *store.StoreManager
	pm  *publish.PublishManager
}

func newSyncer(clients []*rpc.Client, db *gorm.DB, w *kafka.Writer, reversibleBlocks int, storeChannelSize int, storeBatchSize int, storeWorkerCount int, startHeight uint64, endHeight uint64, chainId int64, genesisBlockHash string, messageId uint64) *Syncer {

	logrus.Infof("reversibleBlocks:%v storeChannelSize:%v storeBatchSize:%v storeWorkerCount:%v startHeight:%v endHeight:%v",
		reversibleBlocks, storeChannelSize, storeBatchSize, storeWorkerCount, startHeight, endHeight)

	if startHeight > endHeight {
		logrus.Errorf("start height must be less than end height. startHeight:%v endHeight:%v", startHeight, endHeight)
		os.Exit(0)
	}

	fetch.InitAbi()

	checkNodeChainInfo(clients, chainId, genesisBlockHash)

	logrus.Infof("node chain info check passed")

	storeOperationChannel := make(chan *types.StoreOperation, storeChannelSize)
	publishFeedbackOperationChannel := make(chan *types.PublishFeedbackOperation, 100)

	publishOperationChannel := make(chan *types.PublishOperation, 100)
	pm := publish.NewPublishManager(w, publishOperationChannel, publishFeedbackOperationChannel)

	sm := store.NewStoreManager(db, chainId, messageId, storeBatchSize, storeWorkerCount, storeOperationChannel, publishFeedbackOperationChannel, publishOperationChannel)

	remoteChainUpdateChannel := make(chan *types.RemoteChainUpdate, 100)
	maxUnorganizedBlockCount := 50 * len(clients)
	blkDigestList := loadStartBlock(clients, db, startHeight, reversibleBlocks)

	logrus.Infof("load start block success. count:%v", len(blkDigestList))

	localChain := fetch.NewLocalChain(reversibleBlocks, blkDigestList)
	fm := fetch.NewFetchManager(clients, localChain, endHeight, maxUnorganizedBlockCount, remoteChainUpdateChannel, storeOperationChannel)

	hns := make([]*fetch.HeaderNotifier, len(clients))
	for i, client := range clients {
		hns[i] = fetch.NewHeaderNotifier(i, client, remoteChainUpdateChannel)
	}

	return &Syncer{
		hns: hns,
		fm:  fm,
		sm:  sm,
		pm:  pm,
	}
}

func (s *Syncer) Run() {

	s.pm.Run()
	s.sm.Run()
	s.fm.Run()

	for _, hn := range s.hns {
		hn.Run()
	}
}

func checkNodeChainInfo(clients []*rpc.Client, dbChainId int64, dbGenesisBlockHash string) {

	// compare node chain info with db
	for i, client := range clients {
		strChainId := ""
		if err := client.Call(&strChainId, "eth_chainId"); err != nil {
			logrus.Errorf("get chain id failed. id:%v err:%v", i, err)
			os.Exit(0)
		}

		chainId := hexutil.MustDecodeUint64(strChainId)

		if chainId != uint64(dbChainId) {
			logrus.Errorf("chain id not equal with db. id:%v db:%v node:%v", i, dbChainId, chainId)
			os.Exit(0)
		}

		blkJson := &fetch.BlockHeaderJson{}
		if err := client.Call(blkJson, "eth_getBlockByNumber", "0x0", false); err != nil {
			logrus.Errorf("get genesis block failed. id:%v err:%v", i, err)
			os.Exit(0)
		}

		if blkJson.Hash != dbGenesisBlockHash {
			logrus.Errorf("genesis block not equal with db. id:%v db:%v node:%v", i, dbGenesisBlockHash, blkJson.Hash)
			os.Exit(0)
		}
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

		var fetchHeight uint64
		// meaning from beginning
		if startHeight == 0 || startHeight == math.MaxUint64 {
			fetchHeight = 0
		} else {
			fetchHeight = startHeight - 1
		}
		/*
			fullblock := fetch.FetchFullBlock(0, -1, clients[0], fetchHeight)
			if fullblock == nil {
				logrus.Errorf("startup fetch fullblock failed. height:%v", fetchHeight)
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
		*/

		blockNum := new(big.Int).SetUint64(fetchHeight)
		var blkJson fetch.BlockHeaderJson

		if err := clients[0].Call(&blkJson, "eth_getBlockByNumber", util.ToBlockNumArg(blockNum), false); err != nil {
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
