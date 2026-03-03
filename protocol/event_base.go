package protocol

type EventErc20Transfer struct {
	TxHash       string `json:"tx_hash"`
	EventIndex   uint   `json:"event_index"`
	ContractAddr string `json:"contract_addr"`
	From         string `json:"from"`
	To           string `json:"to"`
	Amount       string `json:"amount"`
}

type EventErc721Transfer struct {
	TxHash       string `json:"tx_hash"`
	EventIndex   uint   `json:"event_index"`
	ContractAddr string `json:"contract_addr"`
	From         string `json:"from"`
	To           string `json:"to"`
	TokenId      string `json:"token_id"`
}

type EventErc1155Transfer struct {
	TxHash       string `json:"tx_hash"`
	EventIndex   uint   `json:"event_index"`
	ContractAddr string `json:"contract_addr"`
	Operator     string `json:"operator"`
	From         string `json:"from"`
	To           string `json:"to"`
	TokenId      string `json:"token_id"`
	Amount       string `json:"amount"`
}
