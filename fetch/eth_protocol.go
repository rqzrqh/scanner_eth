package fetch

import "math/big"

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
	Uncles          []interface{} `json:"uncles"`
}

type BlockJson struct {
	BlockHeaderJson
	Txs []*TxJson `json:"transactions"`
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

type TxInternalJson struct {
	TxHash      string               `json:"transactionHash"`
	BlockHash   string               `json:"blockHash,omitempty"`
	BlockNumber uint64               `json:"blockNumber,omitempty"`
	Logs        []*TxInternalLogJson `json:"logs"`
}

type TxInternalLogJson struct {
	From         string   `json:"From"`
	To           string   `json:"to,omitempty"`
	Value        *big.Int `json:"value,omitempty"`
	Success      bool     `json:"success"`
	OpCode       string   `json:"opcode"`
	Depth        int      `json:"depth"`
	Gas          uint64   `json:"gas"`
	GasUsed      uint64   `json:"gas_used"`
	Input        string   `json:"input"`
	Output       string   `json:"output,omitempty"`
	TraceAddress []uint64 `json:"trace_address"`
}
