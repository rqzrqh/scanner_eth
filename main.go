package main

import (
	"context"
	"flag"
	"fmt"
	syslog "log"
	"net/http"
	"os"
	"os/signal"
	"sync_eth/config"
	"sync_eth/log"
	"sync_eth/model"
	"sync_eth/types"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

var (
	conffile string
	env      string
)

func init() {
	flag.StringVar(&conffile, "conf", "config.yaml", "conf file")
	flag.StringVar(&env, "env", "prd", "[ prd | test ]. Default value: prd")
}

func main() {
	flag.Parse()

	conf, err := config.LoadConf(conffile, env)
	if err != nil {
		fmt.Println("load conf failed.", err)
		os.Exit(0)
	}

	fmt.Println("load config success")

	if err := log.Init(conf.AppName, env, conf.Log); err != nil {
		fmt.Println("log init failed.", err)
		os.Exit(0)
	}

	logrus.Info("init log success")

	newLogger := logger.New(
		syslog.New(os.Stdout, "\r\n", syslog.LstdFlags), // io writer（日志输出的目标，前缀和日志包含的内容——译者注）
		logger.Config{
			SlowThreshold:             1000 * time.Second,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true, // 忽略ErrRecordNotFound（记录未找到）错误
			Colorful:                  true,
		},
	)

	db, err := gorm.Open(mysql.Open(conf.Store.Host), &gorm.Config{
		Logger: newLogger,
	})

	if err != nil {
		logrus.Errorf("failed to connect database %v", err)
		os.Exit(0)
	}

	logrus.Infof("connect database success")

	sqlDB, err := db.DB()
	if err != nil {
		logrus.Errorf("failed to get database instance %v", err)
		os.Exit(0)
	}

	if err := sqlDB.Ping(); err != nil {
		logrus.Errorf("failed to ping database %v", err)
		os.Exit(0)
	}

	logrus.Infof("database ping success")

	if err := db.AutoMigrate(
		&model.ChainInfo{},
		&model.Block{},
		&model.Tx{},
		&model.TxInternal{},
		&model.EventLog{},
		&model.EventErc20Transfer{},
		&model.EventErc721Transfer{},
		&model.EventErc1155Transfer{},

		&model.Contract{},
		&model.ContractErc20{},
		&model.ContractErc721{},
		&model.TokenErc721{},
		&model.Balance{},
		&model.BalanceErc20{},
		&model.BalanceErc1155{},
	); err != nil {
		logrus.Errorf("auto migrate failed %v", err)
		os.Exit(0)
	}

	logrus.Infof("database auto migrate success")

	initChainInfo(db, conf.Chain.ChainId, conf.Chain.GenesisBlockHash)

	logrus.Infof("init chain info success")

	rpcNodeCount := len(conf.Fetch.RpcNodes)
	if rpcNodeCount == 0 {
		logrus.Errorf("rpc node count is zero")
		os.Exit(0)
	}

	clients := make([]*rpc.Client, rpcNodeCount)
	for i, url := range conf.Fetch.RpcNodes {

		customClient := &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
			Timeout: conf.Fetch.Timeout,
		}

		client, err := rpc.DialOptions(context.Background(), url, rpc.WithHTTPClient(customClient))
		if err != nil {
			logrus.Errorf("rpc dial failed idx:%d %v", i, err)
			os.Exit(0)
		}
		clients[i] = client
	}

	logrus.Infof("create rpc client success")

	checkNodeChainInfo(clients, db)

	logrus.Infof("node chain info check passed")

	initGenesisBlock(clients, db)

	logrus.Infof("genesis block init success")

	s := newSyncer(clients, db, conf.Chain.ReversibleBlocks, conf.Store.ChannelSize, conf.Store.BatchSize, conf.Store.WorkerCount, conf.Fetch.StartHeight, conf.Fetch.EndHeight)

	leaseAlive()
	s.Run()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
	<-sigCh
	logrus.Infof("stop sync eth")

	removeFile()
}

var fName = `/tmp/sync_eth.lock`

func removeFile() {
	os.Remove(fName)
}

func leaseAlive() {
	f, err := os.OpenFile(fName, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("create alive file err:%v", err))
	}
	now := time.Now().Unix()
	fmt.Fprintf(f, "%d", now)
}

func initChainInfo(db *gorm.DB, chainId uint64, genesisBlockHash string) {
	var chainInfos []model.ChainInfo
	if err := db.Find(&chainInfos).Error; err != nil {
		logrus.Errorf("load chain info from db failed. err:%v", err)
		os.Exit(0)
	}
	if len(chainInfos) == 0 {
		chainInfo := &model.ChainInfo{
			ChainId:          chainId,
			GenesisBlockHash: genesisBlockHash,
		}
		if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(chainInfo).Error; err != nil {
			logrus.Errorf("insert chain info to db failed. err:%v", err)
			os.Exit(0)
		}
		logrus.Infof("insert chain info to db success. chain_id:%v hash:%v", chainId, genesisBlockHash)
	} else if len(chainInfos) > 1 {
		logrus.Errorf("chain info count more than 1 in db. count:%v", len(chainInfos))
		os.Exit(0)
	} else {
		if chainInfos[0].ChainId != chainId || chainInfos[0].GenesisBlockHash != genesisBlockHash {
			logrus.Errorf("chain info not equal with db. db:%v %v conf:%v %v",
				chainInfos[0].ChainId, chainInfos[0].GenesisBlockHash, chainId, genesisBlockHash)
			os.Exit(0)
		}
	}
}

func checkNodeChainInfo(clients []*rpc.Client, db *gorm.DB) {
	var chainInfo model.ChainInfo
	if err := db.First(&chainInfo).Error; err != nil {
		logrus.Errorf("load chain info from db failed. err:%v", err)
		os.Exit(0)
	}

	// compare node chain info with db
	for i, client := range clients {
		strChainId := ""
		if err := client.Call(&strChainId, "eth_chainId"); err != nil {
			logrus.Errorf("get chain id failed. id:%v err:%v", i, err)
			os.Exit(0)
		}

		chainId := hexutil.MustDecodeUint64(strChainId)

		if chainId != chainInfo.ChainId {
			logrus.Errorf("chain id not equal with db. id:%v db:%v node:%v", i, chainInfo.ChainId, chainId)
			os.Exit(0)
		}

		blkJson := &types.BlockHeaderJson{}
		if err := client.Call(blkJson, "eth_getBlockByNumber", "0x0", false); err != nil {
			logrus.Errorf("get genesis block failed. id:%v err:%v", i, err)
			os.Exit(0)
		}

		if blkJson.Hash != chainInfo.GenesisBlockHash {
			logrus.Errorf("genesis block not equal with db. id:%v db:%v node:%v", i, chainInfo.GenesisBlockHash, blkJson.Hash)
			os.Exit(0)
		}
	}
}

func initGenesisBlock(clients []*rpc.Client, db *gorm.DB) {

	// check height 0 block exist
	var count int64
	if err := db.Model(&model.Block{}).Where("height = ?", 0).Count(&count).Error; err != nil {
		logrus.Errorf("check genesis block exist failed. err:%v", err)
		os.Exit(0)
	}

	if count > 0 {
		logrus.Infof("genesis block exist in db. skip init")
		return
	}

	blkJson := &types.BlockHeaderJson{}
	if err := clients[0].Call(blkJson, "eth_getBlockByNumber", "0x0", false); err != nil {
		logrus.Errorf("get genesis block failed. err:%v", err)
		os.Exit(0)
	}

	genesisBlock := &model.Block{
		Height:    0,
		BlockHash: blkJson.Hash,
	}

	if err := db.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(genesisBlock).Error; err != nil {
		logrus.Errorf("insert genesis block to db failed. err:%v", err)
		os.Exit(0)
	}
}
