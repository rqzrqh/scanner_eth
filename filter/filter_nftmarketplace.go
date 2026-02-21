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

const jsonStrNftMarketplaceABI = `[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"previousAdmin","type":"address"},{"indexed":false,"internalType":"address","name":"newAdmin","type":"address"}],"name":"AdminChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"batchOfferId","type":"uint256"},{"indexed":true,"internalType":"address","name":"seller","type":"address"},{"indexed":true,"internalType":"address","name":"buyer","type":"address"},{"indexed":false,"internalType":"address","name":"nftContract","type":"address"},{"indexed":false,"internalType":"uint256[]","name":"tokenIds","type":"uint256[]"},{"indexed":false,"internalType":"uint256","name":"totalValue","type":"uint256"},{"indexed":false,"internalType":"address","name":"paymentToken","type":"address"}],"name":"BatchOfferAccepted","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"batchOfferId","type":"uint256"},{"indexed":true,"internalType":"address","name":"buyer","type":"address"}],"name":"BatchOfferCancelled","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"batchOfferId","type":"uint256"},{"indexed":true,"internalType":"address","name":"buyer","type":"address"},{"indexed":true,"internalType":"address","name":"nftContract","type":"address"},{"indexed":false,"internalType":"uint256","name":"quantity","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"pricePerItem","type":"uint256"},{"indexed":false,"internalType":"address","name":"paymentToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"totalValue","type":"uint256"}],"name":"BatchOfferMade","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"buyer","type":"address"},{"indexed":true,"internalType":"address","name":"nftContract","type":"address"},{"indexed":false,"internalType":"uint256[]","name":"tokenIds","type":"uint256[]"},{"indexed":false,"internalType":"uint256","name":"totalPrice","type":"uint256"},{"indexed":false,"internalType":"address","name":"paymentToken","type":"address"}],"name":"BatchPurchase","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"beacon","type":"address"}],"name":"BeaconUpgraded","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"newRecipient","type":"address"}],"name":"FeeRecipientUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"FeesWithdrawn","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint8","name":"version","type":"uint8"}],"name":"Initialized","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"listingId","type":"bytes32"},{"indexed":true,"internalType":"address","name":"seller","type":"address"},{"indexed":true,"internalType":"address","name":"nftContract","type":"address"},{"indexed":false,"internalType":"uint256","name":"tokenId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"address","name":"paymentToken","type":"address"}],"name":"ItemListed","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"listingId","type":"bytes32"},{"indexed":true,"internalType":"address","name":"buyer","type":"address"},{"indexed":true,"internalType":"address","name":"seller","type":"address"},{"indexed":false,"internalType":"address","name":"nftContract","type":"address"},{"indexed":false,"internalType":"uint256","name":"tokenId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"address","name":"paymentToken","type":"address"}],"name":"ItemSold","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"listingId","type":"bytes32"},{"indexed":true,"internalType":"address","name":"seller","type":"address"}],"name":"ListingCancelled","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"newFee","type":"uint256"}],"name":"MarketplaceFeeUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"offerId","type":"uint256"},{"indexed":true,"internalType":"address","name":"seller","type":"address"},{"indexed":true,"internalType":"address","name":"buyer","type":"address"},{"indexed":false,"internalType":"address","name":"nftContract","type":"address"},{"indexed":false,"internalType":"uint256","name":"tokenId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"address","name":"paymentToken","type":"address"}],"name":"OfferAccepted","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"offerId","type":"uint256"},{"indexed":true,"internalType":"address","name":"buyer","type":"address"}],"name":"OfferCancelled","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"offerId","type":"uint256"},{"indexed":true,"internalType":"address","name":"buyer","type":"address"},{"indexed":true,"internalType":"address","name":"nftContract","type":"address"},{"indexed":false,"internalType":"uint256","name":"tokenId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"address","name":"paymentToken","type":"address"}],"name":"OfferMade","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"listingId","type":"bytes32"},{"indexed":false,"internalType":"uint256","name":"newPrice","type":"uint256"}],"name":"PriceUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"implementation","type":"address"}],"name":"Upgraded","type":"event"},{"inputs":[],"name":"MAX_MARKETPLACE_FEE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"batchOfferId","type":"uint256"},{"internalType":"uint256[]","name":"tokenIds","type":"uint256[]"}],"name":"acceptBatchOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"offerId","type":"uint256"}],"name":"acceptOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32[]","name":"listingIds","type":"bytes32[]"}],"name":"batchBuyItems","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256[]","name":"tokenIds","type":"uint256[]"},{"internalType":"address[]","name":"sellers","type":"address[]"}],"name":"batchCheckListingStatus","outputs":[{"internalType":"bool[]","name":"","type":"bool[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256[]","name":"tokenIds","type":"uint256[]"},{"internalType":"address[]","name":"sellers","type":"address[]"}],"name":"batchGetListings","outputs":[{"components":[{"internalType":"address","name":"seller","type":"address"},{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"},{"internalType":"address","name":"paymentToken","type":"address"},{"internalType":"bool","name":"active","type":"bool"},{"internalType":"uint256","name":"listingTime","type":"uint256"}],"internalType":"struct NFTMarketplace.Listing[]","name":"","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"batchOffers","outputs":[{"internalType":"address","name":"buyer","type":"address"},{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"quantity","type":"uint256"},{"internalType":"uint256","name":"pricePerItem","type":"uint256"},{"internalType":"address","name":"paymentToken","type":"address"},{"internalType":"uint256","name":"totalValue","type":"uint256"},{"internalType":"bool","name":"active","type":"bool"},{"internalType":"uint256","name":"offerTime","type":"uint256"},{"internalType":"uint256","name":"filledQuantity","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"listingId","type":"bytes32"}],"name":"buyItem","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"batchOfferId","type":"uint256"}],"name":"cancelBatchOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"offerId","type":"uint256"}],"name":"cancelOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"listingId","type":"bytes32"}],"name":"delistItem","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"feeRecipient","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"batchOfferId","type":"uint256"}],"name":"getBatchOfferProgress","outputs":[{"internalType":"uint256","name":"filledQuantity","type":"uint256"},{"internalType":"uint256","name":"totalQuantity","type":"uint256"},{"internalType":"uint256","name":"remainingQuantity","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"batchOfferId","type":"uint256"}],"name":"getBatchOfferRemainingQuantity","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"address","name":"seller","type":"address"}],"name":"getListingId","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"address","name":"seller","type":"address"}],"name":"getNFTListing","outputs":[{"components":[{"internalType":"address","name":"seller","type":"address"},{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"},{"internalType":"address","name":"paymentToken","type":"address"},{"internalType":"bool","name":"active","type":"bool"},{"internalType":"uint256","name":"listingTime","type":"uint256"}],"internalType":"struct NFTMarketplace.Listing","name":"listing","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserActiveBatchOffers","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserActiveOffers","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserBatchOffers","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserListings","outputs":[{"internalType":"bytes32[]","name":"","type":"bytes32[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserOffers","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"batchOfferId","type":"uint256"}],"name":"isBatchOfferValid","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"addr","type":"address"}],"name":"isContract","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"address","name":"seller","type":"address"}],"name":"isListingValid","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"address","name":"seller","type":"address"}],"name":"isNFTListed","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"offerId","type":"uint256"}],"name":"isOfferValid","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"}],"name":"listItem","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"listings","outputs":[{"internalType":"address","name":"seller","type":"address"},{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"},{"internalType":"address","name":"paymentToken","type":"address"},{"internalType":"bool","name":"active","type":"bool"},{"internalType":"uint256","name":"listingTime","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"quantity","type":"uint256"},{"internalType":"uint256","name":"pricePerItem","type":"uint256"}],"name":"makeBatchOffer","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"}],"name":"makeOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"marketplaceFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"offers","outputs":[{"internalType":"address","name":"buyer","type":"address"},{"internalType":"address","name":"nftContract","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"},{"internalType":"address","name":"paymentToken","type":"address"},{"internalType":"bool","name":"active","type":"bool"},{"internalType":"uint256","name":"offerTime","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"proxiableUUID","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_feeRecipient","type":"address"}],"name":"setFeeRecipient","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"newFee","type":"uint256"}],"name":"setMarketplaceFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_wethToken","type":"address"}],"name":"setWETH","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"listingId","type":"bytes32"},{"internalType":"uint256","name":"newPrice","type":"uint256"}],"name":"updatePrice","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"}],"name":"upgradeTo","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"upgradeToAndCall","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"userBatchOffers","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"userListingExists","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"userListingIndex","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"userListings","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"userOffers","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"version","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"wethToken","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`

var (
	nftMarketplaceABI             abi.ABI
	nftMarketplaceContractAddress string
)

func InitNftMarketplaceEventFilter(logger *logrus.Logger, contractAddress string) {

	var err error
	nftMarketplaceABI, err = abi.JSON(strings.NewReader(jsonStrNftMarketplaceABI))
	if err != nil {
		logrus.Fatalf("abi.Json error:%v", err)
		os.Exit(0)
	}

	nftMarketplaceContractAddress = strings.ToLower(contractAddress)
}

func FilterNftMarketplaceEvent(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	if events := filterItemListed(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); events != nil {
		return events
	}
	if events := filterItemSold(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); events != nil {
		return events
	}
	if events := filterListingCancelled(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); events != nil {
		return events
	}
	if events := filterPriceUpdated(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); events != nil {
		return events
	}
	if events := filterOfferMade(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); events != nil {
		return events
	}
	if events := filterOfferAccepted(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); events != nil {
		return events
	}
	if events := filterOfferCancelled(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); events != nil {
		return events
	}
	if events := filterBatchOfferMade(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); events != nil {
		return events
	}
	if events := filterBatchOfferAccepted(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); events != nil {
		return events
	}
	if events := filterBatchOfferCancelled(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); events != nil {
		return events
	}
	if events := filterBatchPurchase(txHash, eventLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount); events != nil {
		return events
	}

	return nil
}

func topicToBigInt(topic string) *big.Int {
	return new(big.Int).SetBytes(common.HexToHash(topic).Bytes())
}

func filterItemListed(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	eventName := "ItemListed"

	// TODO check addr
	if !(topic0 == nftMarketplaceABI.Events[eventName].ID.Hex() && topicCount == 4) {
		return nil
	}

	listingId := common.HexToHash(topic1)
	seller := common.HexToAddress(topic2)
	nftContract := common.HexToAddress(topic3)

	var eventNonIndexedData struct {
		TokenId      *big.Int
		Price        *big.Int
		PaymentToken common.Address
	}

	if err := nftMarketplaceABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("nftMarketplace UnpackIntoInterface err:%v", err)
		os.Exit(0)
	}

	event := &types.NftMarketplaceItemListed{
		ListingId:    listingId,
		Seller:       seller,
		NFTContract:  nftContract,
		TokenId:      eventNonIndexedData.TokenId,
		Price:        eventNonIndexedData.Price,
		PaymentToken: eventNonIndexedData.PaymentToken,
	}

	return []interface{}{event}
}

func filterItemSold(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	eventName := "ItemSold"

	if !(topic0 == nftMarketplaceABI.Events[eventName].ID.Hex() && topicCount == 4) {
		return nil
	}

	listingId := common.HexToHash(topic1)
	buyer := common.HexToAddress(topic2)
	seller := common.HexToAddress(topic3)

	var eventNonIndexedData struct {
		NFTContract  common.Address
		TokenId      *big.Int
		Price        *big.Int
		PaymentToken common.Address
	}

	if err := nftMarketplaceABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("nftMarketplace UnpackIntoInterface err:%v", err)
		os.Exit(0)
	}

	evt := &types.ItemSold{
		ListingId:    listingId,
		Buyer:        buyer,
		Seller:       seller,
		NFTContract:  eventNonIndexedData.NFTContract,
		TokenId:      eventNonIndexedData.TokenId,
		Price:        eventNonIndexedData.Price,
		PaymentToken: eventNonIndexedData.PaymentToken,
	}

	return []interface{}{evt}
}

func filterListingCancelled(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	eventName := "ListingCancelled"

	if !(topic0 == nftMarketplaceABI.Events[eventName].ID.Hex() && topicCount == 3) {
		return nil
	}

	evt := &types.NftMarketplaceListingCancelled{
		ListingId: common.HexToHash(topic1),
		Seller:    common.HexToAddress(topic2),
	}

	return []interface{}{evt}
}

func filterPriceUpdated(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	eventName := "PriceUpdated"

	if !(topic0 == nftMarketplaceABI.Events[eventName].ID.Hex() && topicCount == 2) {
		return nil
	}

	var eventNonIndexedData struct {
		NewPrice *big.Int
	}

	if err := nftMarketplaceABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("nftMarketplace UnpackIntoInterface err:%v", err)
		os.Exit(0)
	}

	evt := &types.NftMarketplacePriceUpdated{
		ListingId: common.HexToHash(topic1),
		NewPrice:  eventNonIndexedData.NewPrice,
	}

	return []interface{}{evt}
}

func filterOfferMade(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	eventName := "OfferMade"

	if !(topic0 == nftMarketplaceABI.Events[eventName].ID.Hex() && topicCount == 4) {
		return nil
	}

	var eventNonIndexedData struct {
		TokenId      *big.Int
		Price        *big.Int
		PaymentToken common.Address
	}

	if err := nftMarketplaceABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("nftMarketplace UnpackIntoInterface err:%v", err)
		os.Exit(0)
	}

	evt := &types.NftMarketplaceOffer{
		OfferId:      topicToBigInt(topic1),
		Buyer:        common.HexToAddress(topic2),
		NFTContract:  common.HexToAddress(topic3),
		TokenId:      eventNonIndexedData.TokenId,
		Price:        eventNonIndexedData.Price,
		PaymentToken: eventNonIndexedData.PaymentToken,
	}

	return []interface{}{evt}
}

func filterOfferAccepted(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	eventName := "OfferAccepted"

	if !(topic0 == nftMarketplaceABI.Events[eventName].ID.Hex() && topicCount == 4) {
		return nil
	}

	var eventNonIndexedData struct {
		NFTContract  common.Address
		TokenId      *big.Int
		Price        *big.Int
		PaymentToken common.Address
	}

	if err := nftMarketplaceABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("nftMarketplace UnpackIntoInterface err:%v", err)
		os.Exit(0)
	}

	evt := &types.NftMarketplaceOfferAccepted{
		OfferId:      topicToBigInt(topic1),
		Seller:       common.HexToAddress(topic2),
		Buyer:        common.HexToAddress(topic3),
		NFTContract:  eventNonIndexedData.NFTContract,
		TokenId:      eventNonIndexedData.TokenId,
		Price:        eventNonIndexedData.Price,
		PaymentToken: eventNonIndexedData.PaymentToken,
	}

	return []interface{}{evt}
}

func filterOfferCancelled(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	eventName := "OfferCancelled"

	if !(topic0 == nftMarketplaceABI.Events[eventName].ID.Hex() && topicCount == 3) {
		return nil
	}

	evt := &types.NftMarketplaceOfferCancelled{
		OfferId: topicToBigInt(topic1),
		Buyer:   common.HexToAddress(topic2),
	}

	return []interface{}{evt}
}

func filterBatchOfferMade(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	eventName := "BatchOfferMade"

	if !(topic0 == nftMarketplaceABI.Events[eventName].ID.Hex() && topicCount == 4) {
		return nil
	}

	var eventNonIndexedData struct {
		Quantity     *big.Int
		PricePerItem *big.Int
		PaymentToken common.Address
		TotalValue   *big.Int
	}

	if err := nftMarketplaceABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("nftMarketplace UnpackIntoInterface err:%v", err)
		os.Exit(0)
	}

	evt := &types.NftMarketplaceBatchOffer{
		BatchOfferId: topicToBigInt(topic1),
		Buyer:        common.HexToAddress(topic2),
		NFTContract:  common.HexToAddress(topic3),
		Quantity:     eventNonIndexedData.Quantity,
		PricePerItem: eventNonIndexedData.PricePerItem,
		PaymentToken: eventNonIndexedData.PaymentToken,
		TotalValue:   eventNonIndexedData.TotalValue,
	}

	return []interface{}{evt}
}

func filterBatchOfferAccepted(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	eventName := "BatchOfferAccepted"

	if !(topic0 == nftMarketplaceABI.Events[eventName].ID.Hex() && topicCount == 4) {
		return nil
	}

	var eventNonIndexedData struct {
		NFTContract  common.Address
		TokenIds     []*big.Int
		TotalValue   *big.Int
		PaymentToken common.Address
	}

	if err := nftMarketplaceABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("nftMarketplace UnpackIntoInterface err:%v", err)
		os.Exit(0)
	}

	evt := &types.NftMarketplaceBatchOfferAccepted{
		BatchOfferId: topicToBigInt(topic1),
		Seller:       common.HexToAddress(topic2),
		Buyer:        common.HexToAddress(topic3),
		NFTContract:  eventNonIndexedData.NFTContract,
		TokenIds:     eventNonIndexedData.TokenIds,
		TotalValue:   eventNonIndexedData.TotalValue,
		PaymentToken: eventNonIndexedData.PaymentToken,
	}

	return []interface{}{evt}
}

func filterBatchOfferCancelled(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	eventName := "BatchOfferCancelled"

	if !(topic0 == nftMarketplaceABI.Events[eventName].ID.Hex() && topicCount == 3) {
		return nil
	}

	evt := &types.NftMarketplaceBatchOfferCancelled{
		BatchOfferId: topicToBigInt(topic1),
		Buyer:        common.HexToAddress(topic2),
	}

	return []interface{}{evt}
}

func filterBatchPurchase(txHash string, eventLog *eth_types.Log, contractAddr string, height uint64, topic0, topic1, topic2, topic3 string, topicCount int) []interface{} {
	eventName := "BatchPurchase"

	if !(topic0 == nftMarketplaceABI.Events[eventName].ID.Hex() && topicCount == 3) {
		return nil
	}

	var eventNonIndexedData struct {
		TokenIds     []*big.Int
		TotalPrice   *big.Int
		PaymentToken common.Address
	}

	if err := nftMarketplaceABI.UnpackIntoInterface(&eventNonIndexedData, eventName, []byte(eventLog.Data)); err != nil {
		logrus.Fatalf("nftMarketplace UnpackIntoInterface err:%v", err)
		os.Exit(0)
	}

	evt := &types.NftMarketplaceBatchPurchase{
		Buyer:        common.HexToAddress(topic1),
		NFTContract:  common.HexToAddress(topic2),
		TokenIds:     eventNonIndexedData.TokenIds,
		TotalPrice:   eventNonIndexedData.TotalPrice,
		PaymentToken: eventNonIndexedData.PaymentToken,
	}

	return []interface{}{evt}
}
