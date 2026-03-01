package protocol

type EventErc20Transfer struct {
	IndexInBlock uint   `json:"index_in_block"`
	ContractAddr string `json:"contract_addr"`
	From         string `json:"from"`
	To           string `json:"to"`
	Amount       string `json:"amount"`
}

type EventErc721Transfer struct {
	IndexInBlock uint   `json:"index_in_block"`
	ContractAddr string `json:"contract_addr"`
	From         string `json:"from"`
	To           string `json:"to"`
	TokenId      string `json:"token_id"`
}

type EventErc1155Transfer struct {
	IndexInBlock uint   `json:"index_in_block"`
	IndexInBatch int    `json:"index_in_batch"`
	ContractAddr string `json:"contract_addr"`
	Operator     string `json:"operator"`
	From         string `json:"from"`
	To           string `json:"to"`
	TokenId      string `json:"token_id"`
	Amount       string `json:"amount"`
}
