package protocol

type HybridPublicConfigChanged struct {
	RoyaltyBasisPoints string
	MaxSupply          string
	MaxMintPerWallet   string
	MintPrice          string
	MintStartTime      string
	MintEndTime        string
}

type HybridWhitelistConfigChanged struct {
	RoyaltyBasisPoints string
	MaxSupply          string
	MaxMintPerWallet   string
	WhitelistPrice     string
	PublicPrice        string
	WhitelistStartTime string
	WhitelistEndTime   string
	PublicEndTime      string
}
