package protocol

type NftMarketplaceItemListed struct {
	ListingId    string `json:"listing_id" gorm:"type:varchar(66);uniqueIndex;not null"`
	Seller       string `json:"seller" gorm:"type:varchar(42);not null;index"`
	NFTContract  string `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	TokenId      string `json:"token_id" gorm:"not null"`
	Price        string `json:"price" gorm:"type:varchar(100);not null"`
	PaymentToken string `json:"payment_token" gorm:"type:varchar(42);not null"`
}

type ItemSold struct {
	ListingId    string `json:"listing_id"`
	Buyer        string `json:"buyer"`
	Seller       string `json:"seller"`
	NFTContract  string `json:"nft_contract"`
	TokenId      string `json:"token_id"`
	Price        string `json:"price"`
	PaymentToken string `json:"payment_token"`
}

type NftMarketplaceOffer struct {
	OfferId      string `json:"offer_id" gorm:"uniqueIndex;not null"`
	Buyer        string `json:"buyer" gorm:"type:varchar(42);not null;index"`
	NFTContract  string `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	TokenId      string `json:"token_id" gorm:"not null"`
	Price        string `json:"price" gorm:"type:varchar(100);not null"`
	PaymentToken string `json:"payment_token" gorm:"type:varchar(42);not null"`
}

type NftMarketplaceBatchOffer struct {
	BatchOfferId string `json:"batch_offer_id" gorm:"uniqueIndex;not null"`
	Buyer        string `json:"buyer" gorm:"type:varchar(42);not null;index"`
	NFTContract  string `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	Quantity     string `json:"quantity" gorm:"not null"`
	PricePerItem string `json:"price_per_item" gorm:"type:varchar(100);not null"`
	PaymentToken string `json:"payment_token" gorm:"type:varchar(42);not null"`
	TotalValue   string `json:"total_value" gorm:"type:varchar(100);not null"`
}

type NftMarketplaceListingCancelled struct {
	ListingId string `json:"listing_id" gorm:"type:varchar(66);uniqueIndex;not null"`
	Seller    string `json:"seller" gorm:"type:varchar(42);not null;index"`
}

type NftMarketplacePriceUpdated struct {
	ListingId string `json:"listing_id" gorm:"type:varchar(66);not null;index"`
	NewPrice  string `json:"new_price" gorm:"type:varchar(100);not null"`
}

type NftMarketplaceOfferAccepted struct {
	OfferId      string `json:"offer_id" gorm:"not null;index"`
	Seller       string `json:"seller" gorm:"type:varchar(42);not null;index"`
	Buyer        string `json:"buyer" gorm:"type:varchar(42);not null;index"`
	NFTContract  string `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	TokenId      string `json:"token_id" gorm:"not null"`
	Price        string `json:"price" gorm:"type:varchar(100);not null"`
	PaymentToken string `json:"payment_token" gorm:"type:varchar(42);not null"`
}

type NftMarketplaceOfferCancelled struct {
	OfferId string `json:"offer_id" gorm:"not null;index"`
	Buyer   string `json:"buyer" gorm:"type:varchar(42);not null;index"`
}

type NftMarketplaceBatchOfferAccepted struct {
	BatchOfferId string   `json:"batch_offer_id" gorm:"not null;index"`
	Seller       string   `json:"seller" gorm:"type:varchar(42);not null;index"`
	Buyer        string   `json:"buyer" gorm:"type:varchar(42);not null;index"`
	NFTContract  string   `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	TokenIds     []string `json:"token_ids" gorm:"type:json;serializer:json"`
	TotalValue   string   `json:"total_value" gorm:"type:varchar(100);not null"`
	PaymentToken string   `json:"payment_token" gorm:"type:varchar(42);not null"`
}

type NftMarketplaceBatchOfferCancelled struct {
	BatchOfferId string `json:"batch_offer_id" gorm:"not null;index"`
	Buyer        string `json:"buyer" gorm:"type:varchar(42);not null;index"`
}

type NftMarketplaceBatchPurchase struct {
	Buyer        string   `json:"buyer" gorm:"type:varchar(42);not null;index"`
	NFTContract  string   `json:"nft_contract" gorm:"type:varchar(42);not null;index"`
	TokenIds     []string `json:"token_ids" gorm:"type:json;serializer:json"`
	TotalPrice   string   `json:"total_price" gorm:"type:varchar(100);not null"`
	PaymentToken string   `json:"payment_token" gorm:"type:varchar(42);not null"`
}
