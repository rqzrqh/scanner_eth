package fetch

import (
	"context"
	"sync_eth/types"
	"sync_eth/util"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
)

type BlockDigestJson struct {
	Hash            string `json:"hash"`
	Number          string `json:"number"`
	TotalDifficulty string `json:"totalDifficulty,omitempty"`
}

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
	if ds.client.SupportsSubscriptions() {
		logrus.Infof("datasource use websocket. id:%d", ds.id)
		ds.useWebsocket()
	} else {
		logrus.Infof("datasource use http. id:%d", ds.id)
		ds.useHttp()
	}
}

func (ds *DataSource) useWebsocket() {
	//newHeadChannel := make(chan *ethTypes.Header, 5)
	newHeadChannel := make(chan *ethTypes.Header, 5)
	defer close(newHeadChannel)

	go func() {
	RECONNECT:
		{
			sub, err := ds.client.Subscribe(context.Background(), "eth", newHeadChannel, "newHeads")
			if err != nil {
				logrus.Warnf("datasource failed to subscribe newHeads. wait reconnect. id:%d error:%v", ds.id, err)
				time.Sleep(3000 * time.Millisecond)
				goto RECONNECT
			}

			logrus.Infof("datasource subscribe newHeads success. id:%d", ds.id)

			for {
				select {
				case err := <-sub.Err():
					logrus.Warnf("datasource newHeads subscription error. id:%d error:%v", ds.id, err)
					sub.Unsubscribe()
					goto RECONNECT
				case header := <-newHeadChannel:
					if header == nil {
						logrus.Warnf("datasource newHeads get nil header. wait reconnect. id:%d", ds.id)
						sub.Unsubscribe()
						time.Sleep(3000 * time.Millisecond)
						goto RECONNECT
					}
					logrus.Infof("datasource new header. id:%d number:%v hash:%v", ds.id, header.Number.String(), header.Hash().Hex())
					ds.remote.Update(header.Number.Uint64(), header.Hash().Hex())
					ds.remoteChainUpdateChannel <- &types.RemoteChainUpdate{
						NodeId:    ds.id,
						Height:    header.Number.Uint64(),
						BlockHash: header.Hash().Hex(),
						Weight:    0,
					}
				}
			}
		}
	}()
}

func (ds *DataSource) useHttp() {
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
			weight := uint64(0)
			if blkJson.TotalDifficulty != "" {
				weight = hexutil.MustDecodeUint64(blkJson.TotalDifficulty)
			}

			ds.remote.Update(height, blockHash)

			ds.remoteChainUpdateChannel <- &types.RemoteChainUpdate{
				NodeId:    ds.id,
				Height:    height,
				BlockHash: blockHash,
				Weight:    weight,
			}
		}
	}()
}
