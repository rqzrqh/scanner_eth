package fetch

import (
	"math"
	"sync_eth/util"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
)

type DataSource struct {
	client         *rpc.Client
	remote         *RemoteChain
	endBlockHeight uint64
	endBlockHash   string
}

func NewDataSource(client *rpc.Client, endBlockHeight uint64, endBlockHash string) *DataSource {
	return &DataSource{
		client:         client,
		remote:         NewRemoteChain(),
		endBlockHeight: endBlockHeight,
		endBlockHash:   endBlockHash,
	}
}

func (ds *DataSource) Run() {
	if ds.endBlockHeight != math.MaxUint64 {
		ds.remote.Update(ds.endBlockHeight, ds.endBlockHash)
		return
	}

	type BlockDigestJson struct {
		Hash   string `json:"hash"`
		Number string `json:"number"`
	}

	go func() {
		for {
			time.Sleep(2000 * time.Millisecond)

			blkJson := &BlockDigestJson{}

			err := ds.client.Call(blkJson, "eth_getBlockByNumber", util.ToBlockNumArg(nil), false)
			if err != nil {
				logrus.Warnf("datasource failed to get latest block", err)
				continue
			}

			if blkJson == nil || blkJson.Number == "" {
				logrus.Warnf("datasource get empty block")
				continue
			}

			height := hexutil.MustDecodeUint64(blkJson.Number)
			blockHash := blkJson.Hash

			ds.remote.Update(height, blockHash)
		}
	}()
}

func (ds *DataSource) GetRemoteChain() *RemoteChain {
	return ds.remote
}
