package data

type NftMarketplaceItemListed struct {
	ListingId    string
	Seller       string
	NFTContract  string
	TokenId      string
	Price        string
	PaymentToken string
}

type ItemSold struct {
	ListingId    string
	Buyer        string
	Seller       string
	NFTContract  string
	TokenId      string
	Price        string
	PaymentToken string
}

type NftMarketplaceOffer struct {
	OfferId      string
	Buyer        string
	NFTContract  string
	TokenId      string
	Price        string
	PaymentToken string
}

type NftMarketplaceBatchOffer struct {
	BatchOfferId string
	Buyer        string
	NFTContract  string
	Quantity     string
	PricePerItem string
	PaymentToken string
	TotalValue   string
}

type NftMarketplaceListingCancelled struct {
	ListingId string
	Seller    string
}

type NftMarketplacePriceUpdated struct {
	ListingId string
	NewPrice  string
}

type NftMarketplaceOfferAccepted struct {
	OfferId      string
	Seller       string
	Buyer        string
	NFTContract  string
	TokenId      string
	Price        string
	PaymentToken string
}

type NftMarketplaceOfferCancelled struct {
	OfferId string
	Buyer   string
}

type NftMarketplaceBatchOfferAccepted struct {
	BatchOfferId string
	Seller       string
	Buyer        string
	NFTContract  string
	TokenIds     []string
	TotalValue   string
	PaymentToken string
}

type NftMarketplaceBatchOfferCancelled struct {
	BatchOfferId string
	Buyer        string
}

type NftMarketplaceBatchPurchase struct {
	Buyer        string
	NFTContract  string
	TokenIds     []string
	TotalPrice   string
	PaymentToken string
}
