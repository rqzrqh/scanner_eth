package protocol

type MemeTokenLaunched struct {
	TxHash        string `json:"tx_hash"`
	EventIndex    uint   `json:"event_index"`
	Token         string `json:"token"`
	Name          string `json:"name"`
	Symbol        string `json:"symbol"`
	Creator       string `json:"creator"`
	VReserveEth   string `json:"v_reserve_eth"`
	VReserveToken string `json:"v_reserve_token"`
	TotalSupply   string `json:"total_supply"`
	AutoBuyAmount string `json:"auto_buy_amount"`
	Pair          string `json:"pair"`
}

type MemeTrade struct {
	TxHash        string `json:"tx_hash"`
	EventIndex    uint   `json:"event_index"`
	Token         string `json:"token"`
	EthAmount     string `json:"eth_amount"`
	EthFeeAmount  string `json:"eth_fee_amount"`
	TokenAmount   string `json:"token_amount"`
	IsBuy         bool   `json:"is_buy"`
	User          string `json:"user"`
	VReserveEth   string `json:"v_reserve_eth"`
	VReserveToken string `json:"v_reserve_token"`
}

type MemeLiquiditySwapped struct {
	TxHash        string `json:"tx_hash"`
	EventIndex    uint   `json:"event_index"`
	Token         string `json:"token"`
	EthAmount     string `json:"eth_amount"`
	TokenAmount   string `json:"token_amount"`
	VReserveEth   string `json:"v_reserve_eth"`
	VReserveToken string `json:"v_reserve_token"`
}
