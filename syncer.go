package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"scanner_eth/config"
	"scanner_eth/fetch"
	"scanner_eth/filter"
	"scanner_eth/publish"

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

	pm := publish.NewPublishManager(conf.Chain.ChainName, db, redisClient, w)

	taskPoolOptions := fetch.TaskPoolOptions{
		WorkerCount:      conf.Fetch.TaskPool.WorkerCount,
		HighQueueSize:    conf.Fetch.TaskPool.HighQueueSize,
		NormalQueueSize:  conf.Fetch.TaskPool.NormalQueueSize,
		MaxRetry:         conf.Fetch.TaskPool.MaxRetry,
		StatsLogInterval: conf.Fetch.TaskPool.StatsLogInterval,
	}
	logrus.Infof("taskPoolConfig workerCount:%v highQueueSize:%v normalQueueSize:%v maxRetry:%v statsLogInterval:%v",
		taskPoolOptions.WorkerCount,
		taskPoolOptions.HighQueueSize,
		taskPoolOptions.NormalQueueSize,
		taskPoolOptions.MaxRetry,
		taskPoolOptions.StatsLogInterval,
	)

	dbOperator := fetch.NewDbOperator(db, reversibleBlocks)
	blockFetcher := fetch.NewBlockFetcher(db)

	fm := fetch.NewFetchManager(
		conf.Chain.ChainName,
		clients,
		redisClient,
		startHeight,
		endHeight,
		reversibleBlocks,
		taskPoolOptions,
		db,
		conf.Chain.ChainId,
		dbOperator,
		blockFetcher,
	)
	if conf.Metrics.Enable {
		fm.EnableTaskPoolMetrics(fmt.Sprintf("fetch_task_pool_%s", conf.Chain.ChainName))
	}

	return &Syncer{
		fm: fm,
		pm: pm,
	}
}

func (s *Syncer) Run() {

	s.pm.Run()
	s.fm.Run()
}

func (s *Syncer) Stop() {
	if s == nil {
		return
	}
	if s.pm != nil {
		s.pm.Stop()
	}
	if s.fm != nil {
		s.fm.Stop()
	}
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
