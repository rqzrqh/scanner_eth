package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type NftMarketplaceItemListed struct {
	ListingId    common.Hash    `json:"listing_id" gorm:"type:varchar(66);uniqueIndex;not null"`
	Seller       common.Address `json:"seller" gorm:"type:varchar(42);not null;index"`
	NFTContract  common.Address `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	TokenId      *big.Int       `json:"token_id" gorm:"not null"`
	Price        *big.Int       `json:"price" gorm:"type:varchar(100);not null"`
	PaymentToken common.Address `json:"payment_token" gorm:"type:varchar(42);not null"`
}

type ItemSold struct {
	ListingId    common.Hash    `json:"listing_id"`
	Buyer        common.Address `json:"buyer"`
	Seller       common.Address `json:"seller"`
	NFTContract  common.Address `json:"nft_contract"`
	TokenId      *big.Int       `json:"token_id"`
	Price        *big.Int       `json:"price"`
	PaymentToken common.Address `json:"payment_token"`
}

type NftMarketplaceOffer struct {
	OfferId      *big.Int       `json:"offer_id" gorm:"uniqueIndex;not null"`
	Buyer        common.Address `json:"buyer" gorm:"type:varchar(42);not null;index"`
	NFTContract  common.Address `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	TokenId      *big.Int       `json:"token_id" gorm:"not null"`
	Price        *big.Int       `json:"price" gorm:"type:varchar(100);not null"`
	PaymentToken common.Address `json:"payment_token" gorm:"type:varchar(42);not null"`
}

type NftMarketplaceBatchOffer struct {
	BatchOfferId *big.Int       `json:"batch_offer_id" gorm:"uniqueIndex;not null"`
	Buyer        common.Address `json:"buyer" gorm:"type:varchar(42);not null;index"`
	NFTContract  common.Address `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	Quantity     *big.Int       `json:"quantity" gorm:"not null"`
	PricePerItem *big.Int       `json:"price_per_item" gorm:"type:varchar(100);not null"`
	PaymentToken common.Address `json:"payment_token" gorm:"type:varchar(42);not null"`
	TotalValue   *big.Int       `json:"total_value" gorm:"type:varchar(100);not null"`
}

type NftMarketplaceListingCancelled struct {
	ListingId common.Hash    `json:"listing_id" gorm:"type:varchar(66);uniqueIndex;not null"`
	Seller    common.Address `json:"seller" gorm:"type:varchar(42);not null;index"`
}

type NftMarketplacePriceUpdated struct {
	ListingId common.Hash `json:"listing_id" gorm:"type:varchar(66);not null;index"`
	NewPrice  *big.Int    `json:"new_price" gorm:"type:varchar(100);not null"`
}

type NftMarketplaceOfferAccepted struct {
	OfferId      *big.Int       `json:"offer_id" gorm:"not null;index"`
	Seller       common.Address `json:"seller" gorm:"type:varchar(42);not null;index"`
	Buyer        common.Address `json:"buyer" gorm:"type:varchar(42);not null;index"`
	NFTContract  common.Address `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	TokenId      *big.Int       `json:"token_id" gorm:"not null"`
	Price        *big.Int       `json:"price" gorm:"type:varchar(100);not null"`
	PaymentToken common.Address `json:"payment_token" gorm:"type:varchar(42);not null"`
}

type NftMarketplaceOfferCancelled struct {
	OfferId *big.Int       `json:"offer_id" gorm:"not null;index"`
	Buyer   common.Address `json:"buyer" gorm:"type:varchar(42);not null;index"`
}

type NftMarketplaceBatchOfferAccepted struct {
	BatchOfferId *big.Int       `json:"batch_offer_id" gorm:"not null;index"`
	Seller       common.Address `json:"seller" gorm:"type:varchar(42);not null;index"`
	Buyer        common.Address `json:"buyer" gorm:"type:varchar(42);not null;index"`
	NFTContract  common.Address `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	TokenIds     []*big.Int     `json:"token_ids" gorm:"type:json;serializer:json"`
	TotalValue   *big.Int       `json:"total_value" gorm:"type:varchar(100);not null"`
	PaymentToken common.Address `json:"payment_token" gorm:"type:varchar(42);not null"`
}

type NftMarketplaceBatchOfferCancelled struct {
	BatchOfferId *big.Int       `json:"batch_offer_id" gorm:"not null;index"`
	Buyer        common.Address `json:"buyer" gorm:"type:varchar(42);not null;index"`
}

type NftMarketplaceBatchPurchase struct {
	Buyer        common.Address `json:"buyer" gorm:"type:varchar(42);not null;index"`
	NFTContract  common.Address `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	TokenIds     []*big.Int     `json:"token_ids" gorm:"type:json;serializer:json"`
	TotalPrice   *big.Int       `json:"total_price" gorm:"type:varchar(100);not null"`
	PaymentToken common.Address `json:"payment_token" gorm:"type:varchar(42);not null"`
}
