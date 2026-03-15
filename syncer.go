package main

import (
	"context"
	"math"
	"math/big"
	"os"
	"scanner_eth/config"
	"scanner_eth/fetch"
	"scanner_eth/filter"
	"scanner_eth/model"
	"scanner_eth/publish"
	"sort"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type Syncer struct {
	fm *fetch.FetchManager
	pm *publish.PublishManager
}

func newSyncer(conf *config.Config, clients []*ethclient.Client, db *gorm.DB, redisClient *redis.Client, w *kafka.Writer, chainId int64, genesisBlockHash string, messageId uint64, publishedMessageId uint64, optionalTables map[string]struct{}) *Syncer {

	reversibleBlocks := conf.Chain.ReversibleBlocks
	startHeight, endHeight, enableInternalTx := conf.Fetch.StartHeight, conf.Fetch.EndHeight, conf.Fetch.EnableInternalTx

	logrus.Infof("reversibleBlocks:%v", reversibleBlocks)
	logrus.Infof("startHeight:%v endHeight:%v enableInternalTx:%v", startHeight, endHeight, enableInternalTx)

	if startHeight > endHeight {
		logrus.Errorf("start height must be less than end height. startHeight:%v endHeight:%v", startHeight, endHeight)
		os.Exit(0)
	}

	filter.InitBaseFilter()
	filter.InitMemeEventFilter(conf.Fetch.Filter.Meme.ContractAddress)
	filter.InitErc20PaymentEventFilter(conf.Fetch.Filter.Erc20Payment.ContractAddress)
	filter.InitHybridNftEventFilter(conf.Fetch.Filter.HybridNft.ContractAddress)
	filter.InitUniswapV2EventFilter(conf.Fetch.Filter.UniswapV2.RouterAddress)

	fetch.SetEnableInternalTx(enableInternalTx)
	fetch.SetOptionalFeatures(optionalTables)

	fetch.InitStore(db, conf.Fetch.Store.BatchSize, conf.Fetch.Store.WorkerCount)

	checkNodeChainInfo(clients, chainId, genesisBlockHash)

	logrus.Infof("node chain info check passed")

	pm := publish.NewPublishManager(conf.Chain.ChainName, db, redisClient, w, conf.Fetch.Interval, conf.Fetch.ExecuteAgain)

	maxUnorganizedBlockCount := 50 * len(clients)
	blkDigestList := loadLatestBlock(clients, db, startHeight, reversibleBlocks)

	logrus.Infof("load latest block success. count:%v", len(blkDigestList))
	for _, blk := range blkDigestList {
		logrus.Infof("latest block height:%v hash:%v parentHash:%v", blk.Height, blk.Hash, blk.ParentHash)
	}

	localChain := fetch.NewLocalChain(reversibleBlocks, blkDigestList)
	fm := fetch.NewFetchManager(conf.Chain.ChainName, clients, redisClient, conf.Fetch.Interval, conf.Fetch.ExecuteAgain, localChain, endHeight, maxUnorganizedBlockCount, db, conf.Chain.ChainId)

	return &Syncer{
		fm: fm,
		pm: pm,
	}
}

func (s *Syncer) Run() {

	s.pm.Run()
	s.fm.Run()
}

func checkNodeChainInfo(clients []*ethclient.Client, dbChainId int64, dbGenesisBlockHash string) {

	// compare node chain info with db
	for i, client := range clients {
		chainId, err := client.ChainID(context.Background())
		if err != nil {
			logrus.Errorf("get chain id failed. id:%v err:%v", i, err)
			os.Exit(0)
		}

		if chainId.Uint64() != uint64(dbChainId) {
			logrus.Errorf("chain id not equal with db. id:%v db:%v node:%v", i, dbChainId, chainId.Uint64())
			os.Exit(0)
		}

		header, err := client.HeaderByNumber(context.Background(), new(big.Int).SetUint64(0))
		if err != nil {
			logrus.Errorf("get genesis block header failed. id:%v err:%v", i, err)
			os.Exit(0)
		}

		if header.Hash().Hex() != dbGenesisBlockHash {
			logrus.Errorf("genesis block not equal with db. id:%v db:%v node:%v", i, dbGenesisBlockHash, header.Hash().Hex())
			os.Exit(0)
		}
	}
}

func loadLatestBlock(clients []*ethclient.Client, db *gorm.DB, startHeight uint64, reversibleBlocks int) []*fetch.BlockDigest {
	blkDigestList := make([]*fetch.BlockDigest, 0)

	var latestBlockList []*model.Block
	if err := db.Order("height desc").Limit(reversibleBlocks).Find(&latestBlockList).Error; err != nil {
		logrus.Errorf("failed to load latest blocks from db %v", err)
		os.Exit(0)
	}

	if len(latestBlockList) == 0 {

		var fetchHeight uint64
		// meaning from beginning
		if startHeight == 0 || startHeight == math.MaxUint64 {
			fetchHeight = 0
		} else {
			fetchHeight = startHeight - 1
		}

		header, err := clients[0].HeaderByNumber(context.Background(), new(big.Int).SetUint64(fetchHeight))
		if err != nil {
			logrus.Warnf("startup failed to get block header. height:%v err:%v", fetchHeight, err)
			os.Exit(0)
		}

		if header == nil {
			logrus.Warnf("startup get empty block. height:%v", fetchHeight)
			os.Exit(0)
		}

		startBlockHeight := header.Number.Uint64()
		startBlockHash := header.Hash().Hex()
		startBlockParentHash := header.ParentHash.Hex()

		block := &model.Block{
			Height:     startBlockHeight,
			Hash:       startBlockHash,
			ParentHash: startBlockParentHash,
			Complete:   true,
		}

		if err := db.Create(block).Error; err != nil {
			logrus.Errorf("startup insert block to db failed. height:%v err:%v", startBlockHeight, err)
			os.Exit(0)
		}

		blk := &fetch.BlockDigest{
			Height:     startBlockHeight,
			Hash:       startBlockHash,
			ParentHash: startBlockParentHash,
		}
		blkDigestList = append(blkDigestList, blk)

	} else {
		sort.Slice(latestBlockList, func(i, j int) bool {
			return latestBlockList[i].Height < latestBlockList[j].Height
		})
	}

	for _, v := range latestBlockList {
		blk := &fetch.BlockDigest{
			Height:     v.Height,
			Hash:       v.Hash,
			ParentHash: v.ParentHash,
		}
		blkDigestList = append(blkDigestList, blk)
	}

	return blkDigestList
}
