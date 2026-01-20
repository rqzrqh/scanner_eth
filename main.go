package main

import (
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
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
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

	sqlDB, err := db.DB()
	if err != nil {
		logrus.Errorf("failed to get database instance %v", err)
		os.Exit(0)
	}

	if err := sqlDB.Ping(); err != nil {
		logrus.Errorf("failed to ping database %v", err)
		os.Exit(0)
	}

	fmt.Println("sql ping success")

	if err := db.AutoMigrate(
		&model.Block{},
		&model.Tx{},
		&model.TxInternal{},
		&model.EventLog{},
		&model.TxErc20{},
		&model.TxErc721{},
		&model.TxErc1155{},

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

	clients := make([]*rpc.Client, len(conf.Fetch.RpcNodes))
	for i, node := range conf.Fetch.RpcNodes {
		client, err := rpc.DialHTTPWithClient(node, &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
			Timeout: conf.Fetch.Timeout,
		})
		if err != nil {
			logrus.Error(err)
			os.Exit(0)
		}
		clients[i] = client
	}

	logrus.Info("create eth client success")

	s := newSyncer(clients, db, conf.Store.ChannelSize, conf.Store.BatchSize, conf.Store.WorkerCount, conf.Fetch.StartHeight, conf.Fetch.EndHeight)

	leaseAlive()
	s.Run()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
	<-sigCh
	logrus.Info("stop sync eth")

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
