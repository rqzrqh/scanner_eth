package filter

import (
	"encoding/hex"
	"os"
	"scanner_eth/data"
	"strings"

	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	eth_types "github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"
)

const jsonStrErc20PaymentABI = `[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"bytes","name":"memo","type":"bytes"}],"name":"Erc20PaymentTransfer","type":"event"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"bytes","name":"permitData","type":"bytes"},{"internalType":"bytes","name":"memo","type":"bytes"}],"name":"transferToken","outputs":[],"stateMutability":"nonpayable","type":"function"}]`

var (
	erc20PaymentABI        abi.ABI
	paymentContractAddress string
)

func InitErc20PaymentEventFilter(_paymentContractAddress string) {

	var err error
	erc20PaymentABI, err = abi.JSON(strings.NewReader(jsonStrErc20PaymentABI))
	if err != nil {
		logrus.Fatalf("abi.Json error:%v", err)
		os.Exit(0)
	}

	paymentContractAddress = strings.ToLower(_paymentContractAddress)
}

func FilterErc20PaymentEvent(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) interface{} {

	if event := filterErc20PaymentTransfer(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); event != nil {
		return event
	}

	return nil
}

func filterErc20PaymentTransfer(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {

	eventName := "Erc20PaymentTransfer"

	if !(topic0 == erc20PaymentABI.Events[eventName].ID.Hex() && topicCount == 4 && contractAddr == paymentContractAddress) {
		return nil
	}

	token := strings.ToLower(topic1)
	from := strings.ToLower(topic2)
	to := strings.ToLower(topic3)

	eventNonIndexedData := struct {
		Amount *big.Int
		Memo   []byte
	}{}

	if err := erc20PaymentABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("Erc20PaymentTransfer UnpackIntoInterface err:%v", err)
		os.Exit(0)
	}

	event := &data.Erc20PaymentTransferEvent{
		Token:  token,
		From:   from,
		To:     to,
		Amount: eventNonIndexedData.Amount.String(),
		Memo:   hex.EncodeToString(eventNonIndexedData.Memo),
	}

	return []interface{}{event}
}
