package protocol

type UniswapV2Swap struct {
	Pair       string
	Sender     string
	Amount0In  string
	Amount1In  string
	Amount0Out string
	Amount1Out string
	To         string
}
