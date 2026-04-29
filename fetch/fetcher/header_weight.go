package fetcher

import "github.com/ethereum/go-ethereum/common/hexutil"

func HeaderWeight(header *BlockHeaderJson) uint64 {
	if header == nil || header.Difficulty == "" {
		return 0
	}
	v, err := hexutil.DecodeBig(header.Difficulty)
	if err != nil || v == nil || v.Sign() <= 0 {
		return 0
	}
	if !v.IsUint64() {
		return ^uint64(0)
	}
	return v.Uint64()
}
