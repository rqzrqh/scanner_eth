package protocol

type Erc20PaymentTransferEvent struct {
	TxHash     string `json:"tx_hash"`
	EventIndex uint   `json:"event_index"`
	Token      string `json:"token"`
	From       string `json:"from"`
	To         string `json:"to"`
	Amount     string `json:"amount"`
	Memo       string `json:"memo"`
}
