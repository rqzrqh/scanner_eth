package data

type Erc20PaymentTransferEvent struct {
	Token  string
	From   string
	To     string
	Amount string
	Memo   string
}
