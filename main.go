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
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/segmentio/kafka-go"
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

	// show loglevel

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
		&model.ScannerInfo{},
		&model.ChainBinlog{},
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
		&model.BalanceNative{},
		&model.BalanceErc20{},
		&model.BalanceErc1155{},
	); err != nil {
		logrus.Errorf("auto migrate failed %v", err)
		os.Exit(0)
	}

	logrus.Infof("database auto migrate success")

	initScannerInfo(db, conf.Chain.ChainId, conf.Chain.GenesisBlockHash)

	logrus.Infof("init scanner info success")

	initGenesisBlock(db, conf.Chain.GenesisBlockHash)

	logrus.Infof("init genesis block success")

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

	chainId, genesisBlockHash := getScannerInfo(db)

	logrus.Infof("get chain info success. chainId:%v genesisBlockHash:%v", chainId, genesisBlockHash)

	w := &kafka.Writer{
		Addr:         kafka.TCP(conf.Publish.KafkaBrokers...),
		Topic:        conf.Publish.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchBytes:   1024 * 1024,
		BatchTimeout: 1 * time.Second,
		Async:        true,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
	}
	//err = w.Close()

	s := newSyncer(clients, db, w, conf.Chain.ReversibleBlocks, conf.Store.ChannelSize, conf.Store.BatchSize, conf.Store.WorkerCount, conf.Fetch.StartHeight, conf.Fetch.EndHeight)

	leaseAlive()
	s.Run(chainId, genesisBlockHash)

	logrus.Infof("start success")

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

func initScannerInfo(db *gorm.DB, chainId uint64, genesisBlockHash string) {
	var scannerInfos []model.ScannerInfo
	if err := db.Find(&scannerInfos).Error; err != nil {
		logrus.Errorf("load scanner info from db failed. err:%v", err)
		os.Exit(0)
	}
	if len(scannerInfos) == 0 {
		scannerInfo := &model.ScannerInfo{
			ChainId:          chainId,
			GenesisBlockHash: genesisBlockHash,
		}
		if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(scannerInfo).Error; err != nil {
			logrus.Errorf("insert scanner info to db failed. err:%v", err)
			os.Exit(0)
		}
		logrus.Infof("insert scanner info to db success. chain_id:%v hash:%v", chainId, genesisBlockHash)
	} else if len(scannerInfos) > 1 {
		logrus.Errorf("scanner info count more than 1 in db. count:%v", len(scannerInfos))
		os.Exit(0)
	} else {
		if scannerInfos[0].ChainId != chainId || scannerInfos[0].GenesisBlockHash != genesisBlockHash {
			logrus.Errorf("scanner info not equal with db. db:%v %v conf:%v %v",
				scannerInfos[0].ChainId, scannerInfos[0].GenesisBlockHash, chainId, genesisBlockHash)
			os.Exit(0)
		}
	}
}

func initGenesisBlock(db *gorm.DB, genesisBlockHash string) {

	genesisBlock := &model.Block{
		Height:    0,
		BlockHash: genesisBlockHash,
	}

	if err := db.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(genesisBlock).Error; err != nil {
		logrus.Errorf("insert genesis block to db failed. err:%v", err)
		os.Exit(0)
	}
}

func getScannerInfo(db *gorm.DB) (uint64, string) {
	var scannerInfo model.ScannerInfo
	if err := db.First(&scannerInfo).Error; err != nil {
		logrus.Errorf("load scanner info from db failed. err:%v", err)
		os.Exit(0)
	}

	return scannerInfo.ChainId, scannerInfo.GenesisBlockHash
}
