package types

type EventErc20Transfer struct {
	TxHash       string
	IndexInBlock uint
	ContractAddr string
	From         string
	To           string
	Amount       string
}

type EventErc721Transfer struct {
	TxHash       string
	IndexInBlock uint
	ContractAddr string
	From         string
	To           string
	TokenId      string
}

type EventErc1155Transfer struct {
	TxHash       string
	IndexInBlock uint
	IndexInBatch int
	ContractAddr string
	Operator     string
	From         string
	To           string
	TokenId      string
	Amount       string
}
