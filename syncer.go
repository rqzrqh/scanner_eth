package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"scanner_eth/config"
	"scanner_eth/fetch"
	fetchconvert "scanner_eth/fetch/convert"
	fetcherpkg "scanner_eth/fetch/fetcher"
	fetchstore "scanner_eth/fetch/store"
	fetchtask "scanner_eth/fetch/task"
	"scanner_eth/filter"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

func newFetchManager(conf *config.Config, clients []*ethclient.Client, db *gorm.DB, redisClient *redis.Client, chainId int64, genesisBlockHash string, optionalTables map[string]struct{}) *fetch.FetchManager {

	reversibleBlocks := conf.Chain.ReversibleBlocks
	startHeight, endHeight, enableInternalTx := conf.Fetch.StartHeight, conf.Fetch.EndHeight, conf.Fetch.EnableInternalTx

	logrus.Infof("reversibleBlocks:%v", reversibleBlocks)
	logrus.Infof("startHeight:%v endHeight:%v enableInternalTx:%v", startHeight, endHeight, enableInternalTx)

	if startHeight > endHeight {
		logrus.Errorf("start height must be less than end height. startHeight:%v endHeight:%v", startHeight, endHeight)
		os.Exit(0)
	}

	filter.InitBaseFilter()

	fetcherpkg.SetEnableInternalTx(enableInternalTx)
	fetchconvert.SetOptionalFeatures(optionalTables)

	fetchstore.DefaultRuntime().Init(db, conf.Fetch.Store.BatchSize, conf.Fetch.Store.WorkerCount)

	checkNodeChainInfo(clients, chainId, genesisBlockHash)

	logrus.Infof("node chain info check passed")

	taskPoolOptions := fetchtask.TaskPoolOptions{
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

	dbOperator := fetchstore.NewFullBlockDbOperator(db, conf.Chain.ChainId, reversibleBlocks)
	blockFetcher := fetcherpkg.NewBlockFetcher(db)

	fm := fetch.NewFetchManager(
		conf.Chain.ChainName,
		clients,
		redisClient,
		startHeight,
		endHeight,
		reversibleBlocks,
		conf.Fetch.Timeout,
		taskPoolOptions,
		db,
		conf.Chain.ChainId,
		dbOperator,
		blockFetcher,
	)
	if conf.Metrics.Enable {
		fm.EnableTaskPoolMetrics(fmt.Sprintf("fetch_task_pool_%s", conf.Chain.ChainName))
	}

	return fm
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
