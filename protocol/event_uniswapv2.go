package protocol

type UniswapV2Swap struct {
	TxHash     string `json:"tx_hash"`
	EventIndex uint   `json:"event_index"`
	Pair       string `json:"pair"`
	Sender     string `json:"sender"`
	Amount0In  string `json:"amount0_in"`
	Amount1In  string `json:"amount1_in"`
	Amount0Out string `json:"amount0_out"`
	Amount1Out string `json:"amount1_out"`
	To         string `json:"to"`
}
