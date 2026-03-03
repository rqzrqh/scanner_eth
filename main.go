package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"scanner_eth/config"
	"scanner_eth/log"
	"scanner_eth/model"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	gormv2logrus "github.com/thomas-tacquet/gormv2-logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormlogger "gorm.io/gorm/logger"
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

	log.InitLogger(conf.AppName, env, conf.Log)

	logrus.Infof("init log success")

	logrusLogger := logrus.New()

	opts := gormv2logrus.GormOptions{
		SlowThreshold: 200 * time.Millisecond,
		LogLevel:      gormlogger.Info,
		TruncateLen:   1000,
		LogLatency:    true,
	}

	gormLogger := gormv2logrus.NewGormlog(gormv2logrus.WithGormOptions(opts), gormv2logrus.WithLogrus(logrusLogger))
	gormLogger.LogMode(gormlogger.Warn)

	db, err := gorm.Open(mysql.Open(conf.Store.Host), &gorm.Config{
		Logger: gormLogger,
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

	if conf.Store.AutoCreateTables {
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
	}

	allOptionalTables := make(map[string]struct{}, 0)
	allOptionalTables[model.Tx.TableName(model.Tx{})] = struct{}{}
	allOptionalTables[model.TxInternal.TableName(model.TxInternal{})] = struct{}{}
	allOptionalTables[model.EventLog.TableName(model.EventLog{})] = struct{}{}
	allOptionalTables[model.BalanceNative.TableName(model.BalanceNative{})] = struct{}{}
	allOptionalTables[model.BalanceErc20.TableName(model.BalanceErc20{})] = struct{}{}
	allOptionalTables[model.BalanceErc1155.TableName(model.BalanceErc1155{})] = struct{}{}
	allOptionalTables[model.EventErc20Transfer.TableName(model.EventErc20Transfer{})] = struct{}{}
	allOptionalTables[model.EventErc721Transfer.TableName(model.EventErc721Transfer{})] = struct{}{}
	allOptionalTables[model.EventErc1155Transfer.TableName(model.EventErc1155Transfer{})] = struct{}{}
	allOptionalTables[model.Contract.TableName(model.Contract{})] = struct{}{}
	allOptionalTables[model.ContractErc20.TableName(model.ContractErc20{})] = struct{}{}
	allOptionalTables[model.ContractErc721.TableName(model.ContractErc721{})] = struct{}{}
	allOptionalTables[model.TokenErc721.TableName(model.TokenErc721{})] = struct{}{}

	// check
	logrus.Infof("optional:%v", conf.Store.Optional)
	optionalTables := make(map[string]struct{}, 0)
	for _, table := range conf.Store.Optional {
		if _, exist := allOptionalTables[table]; !exist {
			logrus.Errorf("optional table:%v not exist", table)
			os.Exit(0)
		}
		optionalTables[table] = struct{}{}
	}

	initScannerInfo(db, conf.Chain.ChainId, conf.Chain.GenesisBlockHash)

	logrus.Infof("init scanner info success")

	rpcNodeCount := len(conf.Fetch.RpcNodes)
	if rpcNodeCount == 0 {
		logrus.Errorf("rpc node count is zero")
		os.Exit(0)
	}

	clients := make([]*ethclient.Client, rpcNodeCount)
	for i, url := range conf.Fetch.RpcNodes {

		customClient := &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
			Timeout: conf.Fetch.Timeout,
		}

		rpcClient, err := rpc.DialOptions(context.Background(), url, rpc.WithHTTPClient(customClient))
		if err != nil {
			logrus.Errorf("rpc dial failed idx:%d %v", i, err)
			os.Exit(0)
		}
		clients[i] = ethclient.NewClient(rpcClient)
	}

	logrus.Infof("create eth client success")

	chainId, genesisBlockHash, messageId, publishedMessageId := getScannerInfo(db)

	logrus.Infof("get chain info success. chainId:%v genesisBlockHash:%v messageId:%v publishedMessageId:%v", chainId, genesisBlockHash, messageId, publishedMessageId)

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

	s := newSyncer(conf, clients, db, w, chainId, genesisBlockHash, messageId, publishedMessageId, optionalTables)
	s.Run()

	logrus.Infof("start success")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
	<-sigCh
	logrus.Infof("stop scanner eth")
}

func initScannerInfo(db *gorm.DB, chainId int64, genesisBlockHash string) {
	var scannerInfos []model.ScannerInfo
	if err := db.Find(&scannerInfos).Error; err != nil {
		logrus.Errorf("load scanner info from db failed. err:%v", err)
		os.Exit(0)
	}
	if len(scannerInfos) == 0 {
		scannerInfo := &model.ScannerInfo{
			ChainId:            chainId,
			GenesisBlockHash:   genesisBlockHash,
			MessageId:          0,
			PublishedMessageId: 0,
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

func getScannerInfo(db *gorm.DB) (int64, string, uint64, uint64) {
	var scannerInfo model.ScannerInfo
	if err := db.First(&scannerInfo).Error; err != nil {
		logrus.Errorf("load scanner info from db failed. err:%v", err)
		os.Exit(0)
	}

	return scannerInfo.ChainId, scannerInfo.GenesisBlockHash, scannerInfo.MessageId, scannerInfo.PublishedMessageId
}
