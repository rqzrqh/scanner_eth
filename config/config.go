package config

import (
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Chain struct {
	Name             string `mapstructure:"name"`
	ChainId          uint64 `mapstructure:"chain_id"`
	GenesisBlockHash string `mapstructure:"genesis_block_hash"`
	ReversibleBlocks int    `mapstructure:"reversible_blocks"`
}

type Fetch struct {
	RpcNodes    []string      `mapstructure:"rpc_nodes"`
	Timeout     time.Duration `mapstructure:"timeout"`
	StartHeight uint64        `mapstructure:"start_height"`
	EndHeight   uint64        `mapstructure:"end_height"`
}

type Store struct {
	Host             string `mapstructure:"host"`
	ChannelSize      int    `mapstructure:"channel_size"`
	BatchSize        int    `mapstructure:"batch_size"`
	WorkerCount      int    `mapstructure:"worker"`
	AutoCreateTables bool   `mapstructure:"auto_create_tables"`
}

type stdout struct {
	Enable bool `mapstructure:"enable"`
	Level  int  `mapstructure:"level"`
}

type file struct {
	Enable bool   `mapstructure:"enable"`
	Path   string `mapstructure:"path"`
	Level  int    `mapstructure:"level"`
}

type kafka struct {
	Enable  bool     `mapstructure:"enable"`
	Level   int      `mapstructure:"level"`
	Brokers []string `mapstructure:"kafka_servers"`
	Topic   string   `mapstructure:"topic"`
}

type Log struct {
	Stdout stdout `mapstructure:"stdout"`
	File   file   `mapstructure:"stdout"`
	Kafka  kafka  `mapstructure:"kafka"`
}

type Config struct {
	AppName string `mapstructure:"stdout"`
	Chain   Chain  `mapstructure:"chain"`
	Fetch   Fetch  `mapstructure:"fetch"`
	Store   Store  `mapstructure:"store"`
	Log     Log    `mapstructure:"log"`
}

func readConfig(filename string, v *viper.Viper) error {
	path, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("err : %v", err)
	}

	v.AddConfigPath(path)
	v.SetConfigName(filename)

	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("read conf file err:%v", err)
	}

	return nil
}

func LoadConf(fpath string, env string) (*Config, error) {
	if fpath == "" {
		return nil, fmt.Errorf("fpath empty")
	}

	if !strings.HasSuffix(strings.ToLower(fpath), "yaml") {
		return nil, fmt.Errorf("fpath must has suffix of .yaml")
	}

	conf := &Config{
		Fetch: Fetch{
			Timeout:     5 * time.Second,
			StartHeight: math.MaxUint64,
			EndHeight:   math.MaxUint64,
		},
		Store: Store{
			ChannelSize:      50,
			BatchSize:        20,
			WorkerCount:      16,
			AutoCreateTables: false,
		},
	}
	vip := viper.New()
	vip.SetConfigType("yaml")

	fmt.Println("read config from local yaml file:", fpath)
	if err := readConfig(fpath, vip); err != nil {
		return nil, err
	}

	err := vip.Unmarshal(conf)
	if err != nil {
		return nil, err
	}

	return conf, nil
}
