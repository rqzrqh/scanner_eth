package filter

import (
	"math/big"
	"os"
	"scanner_eth/types"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	eth_types "github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"
)

const jsonStrMemeABI = `[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[{"internalType":"address","name":"target","type":"address"}],"name":"AddressEmptyCode","type":"error"},{"inputs":[{"internalType":"address","name":"implementation","type":"address"}],"name":"ERC1967InvalidImplementation","type":"error"},{"inputs":[],"name":"ERC1967NonPayable","type":"error"},{"inputs":[],"name":"FailedCall","type":"error"},{"inputs":[],"name":"InvalidInitialization","type":"error"},{"inputs":[],"name":"NotInitializing","type":"error"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"OwnableInvalidOwner","type":"error"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"OwnableUnauthorizedAccount","type":"error"},{"inputs":[],"name":"ReentrancyGuardReentrantCall","type":"error"},{"inputs":[],"name":"UUPSUnauthorizedCallContext","type":"error"},{"inputs":[{"internalType":"bytes32","name":"slot","type":"bytes32"}],"name":"UUPSUnsupportedProxiableUUID","type":"error"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint64","name":"version","type":"uint64"}],"name":"Initialized","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},{"indexed":true,"internalType":"address","name":"pair","type":"address"},{"indexed":false,"internalType":"uint256","name":"lpAmount","type":"uint256"}],"name":"LPBurned","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"tokenAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"ethAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"vReserveEth","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"vReserveToken","type":"uint256"}],"name":"LiquiditySwapped","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"oldValue","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"newValue","type":"uint256"}],"name":"MaxBuyRateWhenCreatedUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"oldValue","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"newValue","type":"uint256"}],"name":"MigrationFeeUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},{"indexed":true,"internalType":"address","name":"user","type":"address"},{"indexed":false,"internalType":"uint256","name":"refundAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"ethAmount","type":"uint256"}],"name":"MigrationRefund","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"oldValue","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"newValue","type":"uint256"}],"name":"MigrationTargetValueUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"string","name":"name","type":"string"},{"indexed":false,"internalType":"string","name":"symbol","type":"string"},{"indexed":true,"internalType":"address","name":"creator","type":"address"},{"indexed":false,"internalType":"uint256","name":"vReserveEth","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"vReserveToken","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"totalSupply","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"autoBuyAmount","type":"uint256"},{"indexed":false,"internalType":"address","name":"pair","type":"address"}],"name":"TokenLaunched","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"ethAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"ethFeeAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"tokenAmount","type":"uint256"},{"indexed":false,"internalType":"bool","name":"isBuy","type":"bool"},{"indexed":true,"internalType":"address","name":"user","type":"address"},{"indexed":false,"internalType":"uint256","name":"vReserveEth","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"vReserveToken","type":"uint256"}],"name":"Trade","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"oldValue","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"newValue","type":"uint256"}],"name":"TradeFeeRateUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"oldValue","type":"address"},{"indexed":false,"internalType":"address","name":"newValue","type":"address"}],"name":"TreasuryWalletUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"implementation","type":"address"}],"name":"Upgraded","type":"event"},{"inputs":[],"name":"BPS_DENOMINATOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_BUY_RATE_WHEN_CREATED","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIGRATION_FEE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIGRATION_TARGET_VALUE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"R_TOKEN_RESERVE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"TOTAL_SUPPLY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"TRADE_FEE_BPS","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"UPGRADE_INTERFACE_VERSION","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"V_ETH_RESERVE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"V_TOKEN_RESERVE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"WETH","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"minTokensOut","type":"uint256"}],"name":"buyToken","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"router","type":"address"},{"internalType":"uint256","name":"TOTAL_SUPPLY","type":"uint256"},{"internalType":"uint256","name":"V_ETH_RESERVE","type":"uint256"},{"internalType":"uint256","name":"V_TOKEN_RESERVE","type":"uint256"},{"internalType":"uint256","name":"R_TOKEN_RESERVE","type":"uint256"},{"internalType":"uint256","name":"MIGRATION_TARGET_VALUE","type":"uint256"},{"internalType":"uint256","name":"MIGRATION_FEE","type":"uint256"},{"internalType":"address","name":"treasuryWallet","type":"address"},{"internalType":"uint256","name":"TRADE_FEE_BPS","type":"uint256"},{"internalType":"uint256","name":"MAX_BUY_RATE_WHEN_CREATED","type":"uint256"}],"internalType":"struct PumpCloneFactory.InitConfig","name":"cfg","type":"tuple"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"_name","type":"string"},{"internalType":"string","name":"_symbol","type":"string"}],"name":"launchBitMemeToken","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"string","name":"_name","type":"string"},{"internalType":"string","name":"_symbol","type":"string"},{"internalType":"address","name":"creator","type":"address"}],"name":"launchBitMemeTokenWithCreator","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"proxiableUUID","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"tokenAmount","type":"uint256"},{"internalType":"uint256","name":"minEthOut","type":"uint256"}],"name":"sellToken","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"tokens","outputs":[{"internalType":"address","name":"creator","type":"address"},{"internalType":"address","name":"tokenAddress","type":"address"},{"internalType":"uint256","name":"vReserveEth","type":"uint256"},{"internalType":"uint256","name":"vReserveToken","type":"uint256"},{"internalType":"uint256","name":"rReserveEth","type":"uint256"},{"internalType":"uint256","name":"rReserveToken","type":"uint256"},{"internalType":"uint256","name":"vLastReserveEth","type":"uint256"},{"internalType":"uint256","name":"vLastReserveToken","type":"uint256"},{"internalType":"uint256","name":"boughtTokensWhenCreated","type":"uint256"},{"internalType":"bool","name":"liquidityMigrated","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"treasuryWallet","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"uniswapRouter","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"updateMaxBuyRateWhenCreated","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"updateMigrationFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"updateMigrationTargetValue","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"updateTradeFeeRate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_treasuryWallet","type":"address"}],"name":"updateTreasuryWallet","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"upgradeToAndCall","outputs":[],"stateMutability":"payable","type":"function"}]`

var (
	memeABI             abi.ABI
	memeContractAddress string
)

func InitMemeEventFilter(_memeContractAddress string) {

	var err error
	memeABI, err = abi.JSON(strings.NewReader(jsonStrMemeABI))
	if err != nil {
		logrus.Fatalf("abi.Json error:%v", err)
		os.Exit(0)
	}

	memeContractAddress = strings.ToLower(_memeContractAddress)
}

func FilterMemeEvent(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) interface{} {
	if event := filterTokenLaunched(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); event != nil {
		return event
	}
	if event := filterTrade(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); event != nil {
		return event
	}
	if event := filterLiquiditySwapped(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); event != nil {
		return event
	}

	return nil
}

func filterTokenLaunched(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) *types.MemeTokenLaunched {
	//eventLog := fullEvent.EventLog
	eventName := "TokenLaunched"

	// TODO check addr
	if !(topic0 == memeABI.Events[eventName].ID.Hex() && topicCount == 3) {
		return nil
	}

	token := common.HexToAddress(topic1)
	creator := common.HexToAddress(topic2)

	var eventNonIndexedData struct {
		Name          string
		Symbol        string
		VReserveEth   *big.Int
		VReserveToken *big.Int
		TotalSupply   *big.Int
		AutoBuyAmount *big.Int
		Pair          common.Address
	}

	if err := memeABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("meme UnpackIntoInterface err:%v height:%v", err, height)
		os.Exit(0)
	}

	event := &types.MemeTokenLaunched{
		Token:         token,
		Name:          eventNonIndexedData.Name,
		Symbol:        eventNonIndexedData.Symbol,
		Creator:       creator,
		VReserveEth:   eventNonIndexedData.VReserveEth,
		VReserveToken: eventNonIndexedData.VReserveToken,
		TotalSupply:   eventNonIndexedData.TotalSupply,
		AutoBuyAmount: eventNonIndexedData.AutoBuyAmount,
		Pair:          eventNonIndexedData.Pair,
	}

	return event
}

func filterTrade(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) *types.MemeTrade {

	//eventLog := fullEvent.EventLog
	eventName := "Trade"

	// TODO check addr
	if !(topic0 == memeABI.Events[eventName].ID.Hex() && topicCount == 3) {
		return nil
	}

	token := common.HexToAddress(topic1)
	user := common.HexToAddress(topic2)

	var eventNonIndexedData struct {
		EthAmount     *big.Int
		EthFeeAmount  *big.Int
		TokenAmount   *big.Int
		IsBuy         bool
		VReserveEth   *big.Int
		VReserveToken *big.Int
	}

	if err := memeABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("meme UnpackIntoInterface err:%v height:%v", err, height)
		os.Exit(0)
	}

	event := &types.MemeTrade{
		Token:         token,
		EthAmount:     eventNonIndexedData.EthAmount,
		EthFeeAmount:  eventNonIndexedData.EthFeeAmount,
		TokenAmount:   eventNonIndexedData.TokenAmount,
		IsBuy:         eventNonIndexedData.IsBuy,
		User:          user,
		VReserveEth:   eventNonIndexedData.VReserveEth,
		VReserveToken: eventNonIndexedData.VReserveToken,
	}
	return event
}

func filterLiquiditySwapped(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) *types.MemeLiquiditySwapped {
	eventName := "LiquiditySwapped"

	// TODO check addr
	if !(topic0 == memeABI.Events[eventName].ID.Hex() && topicCount == 2) {
		return nil
	}

	token := common.HexToAddress(topic1)

	var eventNonIndexedData struct {
		TokenAmount   *big.Int
		EthAmount     *big.Int
		VReserveEth   *big.Int
		VReserveToken *big.Int
	}

	if err := memeABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("meme UnpackIntoInterface err:%v height:%v", err, height)
		os.Exit(0)
	}

	event := &types.MemeLiquiditySwapped{
		Token:         token,
		EthAmount:     eventNonIndexedData.EthAmount,
		TokenAmount:   eventNonIndexedData.TokenAmount,
		VReserveEth:   eventNonIndexedData.VReserveEth,
		VReserveToken: eventNonIndexedData.VReserveToken,
	}
	return event
}
