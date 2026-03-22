package fetch

import (
	"testing"

	"scanner_eth/blocktree"
	"scanner_eth/data"
	"scanner_eth/model"
)

func TestSetOptionalFeatures(t *testing.T) {
	all := map[string]struct{}{
		model.Tx{}.TableName():                {},
		model.TxInternal{}.TableName():        {},
		model.EventLog{}.TableName():          {},
		model.BalanceNative{}.TableName():     {},
		model.BalanceErc20{}.TableName():      {},
		model.BalanceErc1155{}.TableName():    {},
		model.EventErc20Transfer{}.TableName(): {},
		model.EventErc721Transfer{}.TableName(): {},
		model.EventErc1155Transfer{}.TableName(): {},
		model.Contract{}.TableName():          {},
		model.ContractErc20{}.TableName():     {},
		model.ContractErc721{}.TableName():    {},
		model.TokenErc721{}.TableName():       {},
	}
	SetOptionalFeatures(all)
	if !(optionalTx && optionalTxInternal && optionalEventLog && optionalBalanceNative && optionalBalanceErc20 && optionalBalanceErc1155 && optionalEventErc20Transfer && optionalEventErc721Transfer && optionalEventErc1155Transfer && optionalContract && optionalContractErc20 && optionalContractErc721 && optionalTokenErc721) {
		t.Fatal("expected all optional flags to be enabled")
	}

	SetOptionalFeatures(map[string]struct{}{})
	if optionalTx || optionalTxInternal || optionalEventLog || optionalBalanceNative || optionalBalanceErc20 || optionalBalanceErc1155 || optionalEventErc20Transfer || optionalEventErc721Transfer || optionalEventErc1155Transfer || optionalContract || optionalContractErc20 || optionalContractErc721 || optionalTokenErc721 {
		t.Fatal("expected all optional flags to be disabled")
	}
}

func TestNewHeaderNotifier(t *testing.T) {
	ch := make(chan *RemoteChainUpdate, 1)
	hn := NewHeaderNotifier(3, nil, ch)
	if hn == nil {
		t.Fatal("NewHeaderNotifier should return non-nil")
	}
	if hn.id != 3 {
		t.Fatalf("unexpected notifier id: %d", hn.id)
	}
	if hn.remote == nil {
		t.Fatal("remote chain state should be initialized")
	}
	if hn.remoteChainUpdateChannel == nil {
		t.Fatal("remoteChainUpdateChannel should be set")
	}
}

func TestConvertStorageAndProtocolFullBlock(t *testing.T) {
	SetOptionalFeatures(map[string]struct{}{
		model.Tx{}.TableName():                 {},
		model.TxInternal{}.TableName():         {},
		model.EventLog{}.TableName():           {},
		model.BalanceNative{}.TableName():      {},
		model.BalanceErc20{}.TableName():       {},
		model.BalanceErc1155{}.TableName():     {},
		model.EventErc20Transfer{}.TableName(): {},
		model.EventErc721Transfer{}.TableName(): {},
		model.EventErc1155Transfer{}.TableName(): {},
		model.Contract{}.TableName():           {},
		model.ContractErc20{}.TableName():      {},
		model.ContractErc721{}.TableName():     {},
		model.TokenErc721{}.TableName():        {},
	})

	full := &data.FullBlock{
		Block: &data.Block{Height: 88, Hash: "0x88", ParentHash: "0x87", TxCount: 1},
		FullTxList: []*data.FullTx{{
			Tx: &data.Tx{TxHash: "0xtx", From: "0x1", To: "0x2", GasLimit: 21000, ExecStatus: 1},
			TxInternalList: []*data.TxInternal{{TxHash: "0xtx", Index: 0, From: "0x1", To: "0x3", OpCode: "CALL", Success: true}},
			FullEventLogList: []*data.FullEventLog{{
				EventLog: &data.EventLog{IndexInBlock: 1, ContractAddr: "0xc", TopicCount: 1, Topic0: "0x01", Data: []byte{0x1}},
				EventErc20Transfer: &data.EventErc20Transfer{ContractAddr: "0xc20", From: "0x1", To: "0x2", Amount: "10"},
				EventErc721Transfer: &data.EventErc721Transfer{ContractAddr: "0xc721", From: "0x1", To: "0x2", TokenId: "1"},
				EventErc1155Transfers: []*data.EventErc1155Transfer{{ContractAddr: "0xc1155", Operator: "0xop", From: "0x1", To: "0x2", TokenId: "1", Amount: "2"}},
			}},
		}},
		StateSet: &data.StateSet{
			ContractList:       []*data.Contract{{TxHash: "0xtx", ContractAddr: "0xca", CreatorAddr: "0x1", ExecStatus: 1}},
			ContractErc20List:  []*data.ContractErc20{{ContractAddr: "0xc20", Name: "T", Symbol: "T", Decimals: 18, TotalSupply: "100"}},
			ContractErc721List: []*data.ContractErc721{{ContractAddr: "0xc721", Name: "N", Symbol: "N"}},
			BalanceNativeList:  []*data.BalanceNative{{Addr: "0x1", Balance: "9", UpdateHeight: 88}},
			BalanceErc20List:   []*data.BalanceErc20{{Addr: "0x1", ContractAddr: "0xc20", Balance: "8", UpdateHeight: 88}},
			BalanceErc1155List: []*data.BalanceErc1155{{Addr: "0x1", ContractAddr: "0xc1155", TokenId: "1", Balance: "7", UpdateHeight: 88}},
			TokenErc721List:    []*data.TokenErc721{{ContractAddr: "0xc721", TokenId: "1", OwnerAddr: "0x1", TokenUri: "uri", TokenMetaData: []byte("{}"), UpdateHeight: 88}},
		},
	}

	irr := blocktree.IrreversibleNode{Height: 80, Key: "0x80"}
	storage := ConvertStorageFullBlock(full, irr)
	if storage == nil || storage.Block.Height != 88 || storage.Block.IrreversibleHeight != 80 {
		t.Fatalf("unexpected storage conversion result: %+v", storage)
	}
	if len(storage.TxList) != 1 || len(storage.TxInternalList) != 1 || len(storage.EventLogList) != 1 {
		t.Fatal("expected tx/internal/event conversions to be populated")
	}
	if len(storage.ContractErc20List) != 1 || len(storage.ContractErc721List) != 1 || len(storage.TokenErc721List) != 1 {
		t.Fatal("expected state set conversions to be populated")
	}

	protocolBlock := ConvertProtocolFullBlock(full, irr)
	if protocolBlock == nil || protocolBlock.Block == nil || protocolBlock.Block.Height != 88 {
		t.Fatalf("unexpected protocol conversion result: %+v", protocolBlock)
	}
	if protocolBlock.StateSet == nil || len(protocolBlock.StateSet.ContractErc20List) != 1 || len(protocolBlock.StateSet.TokenErc721List) != 1 {
		t.Fatal("expected protocol state set conversions to be populated")
	}
}