package data

type EventErc20Transfer struct {
	ContractAddr string
	From         string
	To           string
	Amount       string
}

type EventErc721Transfer struct {
	ContractAddr string
	From         string
	To           string
	TokenId      string
}

type EventErc1155Transfer struct {
	ContractAddr string
	Operator     string
	From         string
	To           string
	TokenId      string
	Amount       string
}
