package data

type MemeTokenLaunched struct {
	Token         string
	Name          string
	Symbol        string
	Creator       string
	VReserveEth   string
	VReserveToken string
	TotalSupply   string
	AutoBuyAmount string
	Pair          string
}

type MemeTrade struct {
	Token         string
	EthAmount     string
	EthFeeAmount  string
	TokenAmount   string
	IsBuy         bool
	User          string
	VReserveEth   string
	VReserveToken string
}

type MemeLiquiditySwapped struct {
	Token         string
	EthAmount     string
	TokenAmount   string
	VReserveEth   string
	VReserveToken string
}
