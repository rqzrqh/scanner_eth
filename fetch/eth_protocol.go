package fetch

import "github.com/ethereum/go-ethereum/common/hexutil"

type BlockHeaderJson struct {
	BaseFeePerGas   string        `json:"baseFeePerGas"`
	Difficulty      string        `json:"difficulty"`
	ExtraData       string        `json:"extraData"`
	GasLimit        string        `json:"gasLimit"`
	GasUsed         string        `json:"gasUsed"`
	Hash            string        `json:"hash"`
	Miner           string        `json:"miner"`
	Nonce           string        `json:"nonce"`
	Number          string        `json:"number"`
	ParentHash      string        `json:"parentHash"`
	ReceiptsRoot    string        `json:"receiptsRoot"`
	Sha3Uncles      string        `json:"sha3Uncles"`
	Size            string        `json:"size"`
	StateRoot       string        `json:"stateRoot"`
	TimeStamp       string        `json:"timestamp"`
	TotalDifficulty string        `json:"totalDifficulty"`
	TransactionRoot string        `json:"transactionsRoot"`
	Transactions    []string      `json:"transactions"`
	Uncles          []interface{} `json:"uncles"`
}

type TxJson struct {
	Hash                 string `json:"hash"`
	From                 string `json:"from"`
	To                   string `json:"to"`
	Gas                  string `json:"gas"`
	GasPrice             string `json:"gasPrice"`
	Input                string `json:"input"`
	MaxFeePerGas         string `json:"maxFeePerGas"`
	MaxPriorityFeePerGas string `json:"maxPriorityFeePerGas"`
	Nonce                string `json:"nonce"`
	R                    string `json:"r"`
	S                    string `json:"s"`
	V                    string `json:"v"`
	TransactionIndex     string `json:"transactionIndex"`
	Type                 string `json:"type"`
	Value                string `json:"value"`
}

type TxInternalTraceResultJson struct {
	TxHash string          `json:"txHash"`
	Result *TxInternalJson `json:"result"`
	Error  string          `json:"error,omitempty"`
}

type TxInternalJson struct {
	Type         string            `json:"type"`
	From         string            `json:"from"`
	To           string            `json:"to,omitempty"`
	Value        *hexutil.Big      `json:"value,omitempty"`
	Gas          hexutil.Uint64    `json:"gas"`
	GasUsed      hexutil.Uint64    `json:"gasUsed"`
	Input        string            `json:"input"`
	Output       string            `json:"output,omitempty"`
	Error        string            `json:"error,omitempty"`
	RevertReason string            `json:"revertReason,omitempty"`
	Calls        []*TxInternalJson `json:"calls,omitempty"`
}
