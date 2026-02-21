package types

import "math/big"

type HybridPublicConfigChanged struct {
	RoyaltyBasisPoints *big.Int
	MaxSupply          *big.Int
	MaxMintPerWallet   *big.Int
	MintPrice          *big.Int
	MintStartTime      *big.Int
	MintEndTime        *big.Int
}

type HybridWhitelistConfigChanged struct {
	RoyaltyBasisPoints *big.Int
	MaxSupply          *big.Int
	MaxMintPerWallet   *big.Int
	WhitelistPrice     *big.Int
	PublicPrice        *big.Int
	WhitelistStartTime *big.Int
	WhitelistEndTime   *big.Int
	PublicEndTime      *big.Int
}
