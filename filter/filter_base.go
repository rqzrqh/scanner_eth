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

const jsonStrErc20ABI = `[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"deposit","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"},{"name":"","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"guy","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Deposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Withdrawal","type":"event"}]`
const jsonStrErc721ABI = `[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"DODONFTBurn","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"creator","type":"address"},{"indexed":false,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"DODONFTMint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferPrepared","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"_CUR_TOKENID_","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"_NEW_OWNER_","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"_OWNER_","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"claimOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"symbol","type":"string"}],"name":"init","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"initOwner","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"uri","type":"string"}],"name":"mint","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"operator","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"index","type":"uint256"}],"name":"tokenByIndex","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"tokenOfOwnerByIndex","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}]`
const jsonStrErc1155ABI = `[{"inputs":[{"internalType":"address","name":"_logic","type":"address"},{"internalType":"address","name":"admin_","type":"address"},{"internalType":"bytes","name":"_data","type":"bytes"}],"stateMutability":"payable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"previousAdmin","type":"address"},{"indexed":false,"internalType":"address","name":"newAdmin","type":"address"}],"name":"AdminChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"beacon","type":"address"}],"name":"BeaconUpgraded","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"implementation","type":"address"}],"name":"Upgraded","type":"event"},{"stateMutability":"payable","type":"fallback"},{"inputs":[],"name":"admin","outputs":[{"internalType":"address","name":"admin_","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newAdmin","type":"address"}],"name":"changeAdmin","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"implementation","outputs":[{"internalType":"address","name":"implementation_","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"}],"name":"upgradeTo","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"upgradeToAndCall","outputs":[],"stateMutability":"payable","type":"function"},{"stateMutability":"payable","type":"receive"}]`

const erc20Transfer = `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`
const erc1155SingleTransfer = `0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62`
const erc1155BatchTransfer = `0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb`
const erc721Transfer = erc20Transfer

var (
	Erc20ABI, Erc721ABI, Erc1155ABI abi.ABI
)

func InitBaseFilter() {
	var r *strings.Reader
	var err error

	r = strings.NewReader(jsonStrErc20ABI)
	Erc20ABI, err = abi.JSON(r)
	if err != nil {
		os.Exit(0)
	}

	r = strings.NewReader(jsonStrErc721ABI)
	Erc721ABI, err = abi.JSON(r)
	if err != nil {
		os.Exit(0)
	}

	r = strings.NewReader(jsonStrErc1155ABI)
	Erc1155ABI, err = abi.JSON(r)
	if err != nil {
		os.Exit(0)
	}
}

func isErc20TransferEvent(topic0, topic1, topic2, topic3 string) bool {
	return topic0 == erc20Transfer && topic1 != "" && topic2 != "" && topic3 == ""
}

func isErc721TransferEvent(topic0, topic1, topic2, topic3 string) bool {
	return topic0 == erc721Transfer && topic1 != "" && topic2 != "" && topic3 != ""
}

func isErc1155SingleTransferEvent(topic0, topic1, topic2, topic3 string) bool {
	return topic0 == erc1155SingleTransfer && topic1 != "" && topic2 != "" && topic3 != ""
}

func isErc1155BatchTransferEvent(topic0, topic1, topic2, topic3 string) bool {
	return topic0 == erc1155BatchTransfer && topic1 != "" && topic2 != "" && topic3 != ""
}

func FilterErc20TransferEvent(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string) *types.EventErc20Transfer {

	if !isErc20TransferEvent(topic0, topic1, topic2, topic3) {
		return nil
	}
	sender := strings.ToLower(common.HexToAddress(topic1).Hex())
	receiver := strings.ToLower(common.HexToAddress(topic2).Hex())
	tokenAmount := new(big.Int)
	tokenAmount.SetBytes(eventLog.Data)
	/*
		// erc20 balance
		if tokenAmount.Uint64() != 0 {
			if _, ok := balanceErc20Address[sender]; !ok {
				balanceErc20Address[sender] = make(map[string]struct{}, 0)
			}
			if _, ok := balanceErc20Address[receiver]; !ok {
				balanceErc20Address[receiver] = make(map[string]struct{}, 0)
			}

			balanceErc20Address[sender][contractAddr] = struct{}{}
			balanceErc20Address[receiver][contractAddr] = struct{}{}
		}
	*/
	// tx erc20
	eventErc20Transfer := &types.EventErc20Transfer{
		TxHash:       txHash,
		IndexInBlock: uint(eventLog.Index),
		ContractAddr: contractAddr,
		From:         sender,
		To:           receiver,
		Amount:       tokenAmount.String(),
	}

	return eventErc20Transfer
	/*
		eventErc20TransferList = append(eventErc20TransferList, eventErc20Transfer)

		if contractAddr != util.ZeroAddress {
			erc20ContractAddrs[contractAddr] = struct{}{}
		}

		balanceNativeAddress[sender] = struct{}{}
		balanceNativeAddress[receiver] = struct{}{}
		balanceNativeAddress[contractAddr] = struct{}{}
	*/
}

func FilterErc721TransferEvent(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string) *types.EventErc721Transfer {
	if !isErc721TransferEvent(topic0, topic1, topic2, topic3) {
		return nil
	}

	sender := strings.ToLower(common.HexToAddress(topic1).Hex())
	receiver := strings.ToLower(common.HexToAddress(topic2).Hex())
	tokenId := common.HexToHash(topic3).Big().String()

	// tx erc721
	eventErc721Transfer := &types.EventErc721Transfer{
		TxHash:       txHash,
		ContractAddr: contractAddr,
		From:         sender,
		To:           receiver,
		TokenId:      tokenId,
		IndexInBlock: uint(eventLog.Index),
	}
	/*
		eventErc721TransferList = append(eventErc721TransferList, eventErc721Transfer)

		tokenErc721KeyValue := TokenErc721KeyValue{
			ContractAddr: contractAddr,
			TokenId:      tokenId,
		}
		tokenErc721Set[tokenErc721KeyValue] = tokenErc721KeyValue

		if contractAddr != util.ZeroAddress {
			erc721ContractAddrs[contractAddr] = struct{}{}
		}

		balanceNativeAddress[sender] = struct{}{}
		balanceNativeAddress[receiver] = struct{}{}
		balanceNativeAddress[contractAddr] = struct{}{}
	*/

	return eventErc721Transfer
}

func FilterErc1155SingleTransferEvent(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string) *types.EventErc1155Transfer {
	if !isErc1155SingleTransferEvent(topic0, topic1, topic2, topic3) {
		return nil
	}

	var eventNonIndexedData struct {
		Id    *big.Int
		Value *big.Int
	}

	if err := Erc1155ABI.UnpackIntoInterface(&eventNonIndexedData, "TransferSingle", eventLog.Data); err != nil {
		logrus.Errorf("erc1155 single err:%v height:%v", err, height)
		return nil
	}

	operator := strings.ToLower(common.HexToAddress(topic1).Hex())
	sender := strings.ToLower(common.HexToAddress(topic2).Hex())
	receiver := strings.ToLower(common.HexToAddress(topic3).Hex())
	tokenId := eventNonIndexedData.Id.String()
	/*
		// balance erc1155
		if _, ok := balanceErc1155Address[contractAddr]; !ok {
			balanceErc1155Address[contractAddr] = make(map[string]string, 0)
		}

		if sender != util.ZeroAddress {
			balanceErc1155Address[contractAddr][tokenId] = sender
		}
		if receiver != util.ZeroAddress {
			balanceErc1155Address[contractAddr][tokenId] = receiver
		}
	*/
	// tx erc1155
	tokens := eventNonIndexedData.Value
	amount := tokens.String()

	eventErc1155Transfer := &types.EventErc1155Transfer{
		TxHash:       txHash,
		IndexInBlock: uint(eventLog.Index),
		ContractAddr: contractAddr,
		Operator:     operator,
		From:         sender,
		To:           receiver,
		TokenId:      tokenId,
		Amount:       amount,
		IndexInBatch: -1,
	}

	return eventErc1155Transfer
	/*
		eventErc1155TransferList = append(eventErc1155TransferList, eventErc1155Transfer)

		balanceNativeAddress[operator] = struct{}{}
		balanceNativeAddress[sender] = struct{}{}
		balanceNativeAddress[receiver] = struct{}{}
		balanceNativeAddress[contractAddr] = struct{}{}
	*/
}

func FilterErc1155BatchTransferEvent(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string) []*types.EventErc1155Transfer {

	if !isErc1155BatchTransferEvent(topic0, topic1, topic2, topic3) {
		return []*types.EventErc1155Transfer{}
	}

	var eventNonIndexedData struct {
		Ids    []*big.Int
		Values []*big.Int
	}

	if err := Erc1155ABI.UnpackIntoInterface(&eventNonIndexedData, "TransferBatch", eventLog.Data); err != nil {
		logrus.Errorf("erc1155 batch UnpackIntoInterface err:%v height:%v", err, height)
		return []*types.EventErc1155Transfer{}
	}

	ids := eventNonIndexedData.Ids
	values := eventNonIndexedData.Values
	if len(values) != len(ids) {
		logrus.Warnf("erc1155 batch nonstandard tx_hash:%v index:%v height:%v", txHash, eventLog.Index, height)
		return []*types.EventErc1155Transfer{}
	}

	operator := strings.ToLower(common.HexToAddress(topic1).Hex())
	sender := strings.ToLower(common.HexToAddress(topic2).Hex())
	receiver := strings.ToLower(common.HexToAddress(topic3).Hex())

	eventErc1155Transfers := make([]*types.EventErc1155Transfer, 0, len(ids))

	for index, id := range ids {

		tokenId := id.String()

		// tx erc1155
		amount := values[index].String()

		eventErc1155Transfer := &types.EventErc1155Transfer{
			TxHash:       txHash,
			IndexInBlock: uint(eventLog.Index),
			IndexInBatch: index,
			ContractAddr: contractAddr,
			Operator:     operator,
			From:         sender,
			To:           receiver,
			TokenId:      tokenId,
			Amount:       amount,
		}
		eventErc1155Transfers = append(eventErc1155Transfers, eventErc1155Transfer)
	}

	return eventErc1155Transfers
}
