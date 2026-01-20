package fetch

import (
	"sync_eth/types"
	"sync_eth/util"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
)

type DataSource struct {
	id                       int
	client                   *rpc.Client
	remote                   *RemoteChain
	remoteChainUpdateChannel chan<- *types.RemoteChainUpdate
}

func NewDataSource(id int, client *rpc.Client, remoteChainUpdateChannel chan<- *types.RemoteChainUpdate) *DataSource {
	return &DataSource{
		id:                       id,
		client:                   client,
		remote:                   NewRemoteChain(),
		remoteChainUpdateChannel: remoteChainUpdateChannel,
	}
}

func (ds *DataSource) Run() {

	type BlockDigestJson struct {
		Hash   string `json:"hash"`
		Number string `json:"number"`
	}

	go func() {

		for {
			time.Sleep(5000 * time.Millisecond)

			blkJson := &BlockDigestJson{}

			err := ds.client.Call(blkJson, "eth_getBlockByNumber", util.ToBlockNumArg(nil), false)
			if err != nil {
				logrus.Warnf("datasource failed to get latest block. id:%d error:%v", ds.id, err)
				continue
			}

			if blkJson.Number == "" {
				logrus.Warnf("datasource get empty block. id:%d", ds.id)
				continue
			}

			height := hexutil.MustDecodeUint64(blkJson.Number)
			blockHash := blkJson.Hash

			ds.remote.Update(height, blockHash)

			ds.remoteChainUpdateChannel <- &types.RemoteChainUpdate{
				Id:        ds.id,
				Height:    height,
				BlockHash: blockHash,
			}
		}
	}()
}
