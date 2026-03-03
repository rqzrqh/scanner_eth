package protocol

type HybridPublicConfigChanged struct {
	TxHash             string `json:"tx_hash"`
	EventIndex         uint   `json:"event_index"`
	RoyaltyBasisPoints string `json:"royalty_basis_points"`
	MaxSupply          string `json:"max_supply"`
	MaxMintPerWallet   string `json:"max_mint_per_wallet"`
	MintPrice          string `json:"mint_price"`
	MintStartTime      string `json:"mint_start_time"`
	MintEndTime        string `json:"mint_end_time"`
}

type HybridWhitelistConfigChanged struct {
	TxHash             string `json:"tx_hash"`
	EventIndex         uint   `json:"event_index"`
	RoyaltyBasisPoints string `json:"royalty_basis_points"`
	MaxSupply          string `json:"max_supply"`
	MaxMintPerWallet   string `json:"max_mint_per_wallet"`
	WhitelistPrice     string `json:"whitelist_price"`
	PublicPrice        string `json:"public_price"`
	WhitelistStartTime string `json:"whitelist_start_time"`
	WhitelistEndTime   string `json:"whitelist_end_time"`
	PublicEndTime      string `json:"public_end_time"`
}
