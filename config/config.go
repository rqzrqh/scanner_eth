package config

import (
	"fmt"
	"math"
	"os"
	"scanner_eth/middleware"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Chain struct {
	ChainName        string `mapstructure:"chain_name"`
	ChainId          int64  `mapstructure:"chain_id"`
	GenesisBlockHash string `mapstructure:"genesis_block_hash"`
	ReversibleBlocks int    `mapstructure:"reversible_blocks"`
}

type Meme struct {
	ContractAddress string `mapstructure:"contract_address"`
}

type NftMarketplace struct {
	ContractAddress string `mapstructure:"contract_address"`
}

type HybridNft struct {
	ContractAddress string `mapstructure:"contract_address"`
}

type Erc20Payment struct {
	ContractAddress string `mapstructure:"contract_address"`
}

type UniswapV2 struct {
	RouterAddress string `mapstructure:"router_address"`
}

type Filter struct {
	Meme           Meme           `mapstructure:"meme"`
	NftMarketplace NftMarketplace `mapstructure:"nft_marketplace"`
	HybridNft      HybridNft      `mapstructure:"hybrid_nft"`
	Erc20Payment   Erc20Payment   `mapstructure:"erc20_payment"`
	UniswapV2      UniswapV2      `mapstructure:"uniswapv2"`
}

type Store struct {
	AutoCreateTables bool     `mapstructure:"auto_create_tables"`
	Optional         []string `mapstructure:"optional"`
}

type Fetch struct {
	RpcNodes         []string      `mapstructure:"rpc_nodes"`
	Timeout          time.Duration `mapstructure:"timeout"`
	StartHeight      uint64        `mapstructure:"start_height"`
	EndHeight        uint64        `mapstructure:"end_height"`
	EnableInternalTx bool          `mapstructure:"enable_internal_tx"`
	Interval         time.Duration `mapstructure:"interval"`
	ExecuteAgain     bool          `mapstructure:"execute_again"`
	Filter           Filter        `mapstructure:"filter"`
	Store            Store         `mapstructure:"store"`
}

type Kafka struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
}

type Publish struct {
	Interval     time.Duration `mapstructure:"interval"`
	ExecuteAgain bool          `mapstructure:"execute_again"`
}

type console struct {
	Enable bool   `mapstructure:"enable"`
	Level  string `mapstructure:"level"`
}

type file struct {
	Enable     bool   `mapstructure:"enable"`
	Name       string `mapstructure:"name"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAge     int    `mapstructure:"max_age"`
	Level      string `mapstructure:"level"`
}

type Log struct {
	Console console `mapstructure:"console"`
	File    file    `mapstructure:"file"`
}

type Config struct {
	AppName  string                 `mapstructure:"app_name"`
	Chain    Chain                  `mapstructure:"chain"`
	Fetch    Fetch                  `mapstructure:"fetch"`
	Publish  Publish                `mapstructure:"publish"`
	Database middleware.Database    `mapstructure:"database"`
	Redis    middleware.RedisConfig `mapstructure:"redis"`
	Kafka    Kafka                  `mapstructure:"kafka"`
	Log      Log                    `mapstructure:"log"`
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
			Timeout:          5 * time.Second,
			StartHeight:      math.MaxUint64,
			EndHeight:        math.MaxUint64,
			EnableInternalTx: true,
			Store: Store{
				AutoCreateTables: true,
				Optional:         []string{},
			},
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
