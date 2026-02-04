package fetch

import (
	"context"
	"os"
	"sync_eth/types"
	"sync_eth/util"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
)

type HeaderNotifier struct {
	id                       int
	client                   *rpc.Client
	remote                   *RemoteChain
	remoteChainUpdateChannel chan<- *types.RemoteChainUpdate
}

func NewHeaderNotifier(id int, client *rpc.Client, remoteChainUpdateChannel chan<- *types.RemoteChainUpdate) *HeaderNotifier {
	return &HeaderNotifier{
		id:                       id,
		client:                   client,
		remote:                   NewRemoteChain(),
		remoteChainUpdateChannel: remoteChainUpdateChannel,
	}
}

func (ds *HeaderNotifier) Run(dbChainId uint64, dbGenesisBlockHash string) {

	go func() {

		// compare node chain info with db
		for {
			strChainId := ""
			if err := ds.client.Call(&strChainId, "eth_chainId"); err != nil {
				logrus.Warnf("get chain id failed. id:%v err:%v", ds.id, err)
				time.Sleep(3000 * time.Millisecond)
				continue
			}

			chainId := hexutil.MustDecodeUint64(strChainId)

			if chainId != dbChainId {
				logrus.Errorf("chain id not equal with db. id:%v db:%v node:%v", ds.id, dbChainId, chainId)
				os.Exit(0)
			}

			break
		}

		for {
			blkJson := &BlockHeaderJson{}
			if err := ds.client.Call(blkJson, "eth_getBlockByNumber", "0x0", false); err != nil {
				logrus.Warnf("get genesis block failed. id:%v err:%v", ds.id, err)
				time.Sleep(3000 * time.Millisecond)
				continue
			}

			if blkJson.Hash != dbGenesisBlockHash {
				logrus.Errorf("genesis block not equal with db. id:%v db:%v node:%v", ds.id, dbGenesisBlockHash, blkJson.Hash)
				os.Exit(0)
			}

			break
		}

		logrus.Infof("node chain info check passed. id:%v", ds.id)

		if ds.client.SupportsSubscriptions() {
			logrus.Infof("header notifier use websocket. id:%d", ds.id)
			ds.useWebsocket()
		} else {
			logrus.Infof("header notifier use http. id:%d", ds.id)
			ds.useHttp()
		}
	}()
}

func (ds *HeaderNotifier) useWebsocket() {
	//newHeadChannel := make(chan *ethTypes.Header, 5)
	newHeadChannel := make(chan *ethTypes.Header, 5)
	defer close(newHeadChannel)

RECONNECT:
	{
		sub, err := ds.client.Subscribe(context.Background(), "eth", newHeadChannel, "newHeads")
		if err != nil {
			logrus.Warnf("header notifier failed to subscribe newHeads. wait reconnect. id:%d error:%v", ds.id, err)
			time.Sleep(3000 * time.Millisecond)
			goto RECONNECT
		}

		logrus.Infof("header notifier subscribe newHeads success. id:%d", ds.id)

		for {
			select {
			case err := <-sub.Err():
				logrus.Warnf("header notifier newHeads subscription error. id:%d error:%v", ds.id, err)
				sub.Unsubscribe()
				goto RECONNECT
			case header := <-newHeadChannel:
				if header == nil {
					logrus.Warnf("header notifier newHeads get nil header. wait reconnect. id:%d", ds.id)
					sub.Unsubscribe()
					time.Sleep(3000 * time.Millisecond)
					goto RECONNECT
				}

				height := header.Number.Uint64()
				blockHash := header.Hash().Hex()
				weight := uint64(0)

				logrus.Infof("header notifier new header. id:%d height:%v hash:%v", ds.id, height, blockHash)

				ds.remote.Update(height, blockHash)
				ds.remoteChainUpdateChannel <- &types.RemoteChainUpdate{
					NodeId:    ds.id,
					Height:    height,
					BlockHash: blockHash,
					Weight:    weight,
				}
			}
		}
	}
}

func (ds *HeaderNotifier) useHttp() {
	for {
		blkJson := &BlockHeaderJson{}
		err := ds.client.Call(blkJson, "eth_getBlockByNumber", util.ToBlockNumArg(nil), false)
		if err != nil {
			logrus.Warnf("header notifier failed to get latest block. id:%d error:%v", ds.id, err)
			time.Sleep(5000 * time.Millisecond)
			continue
		}

		if blkJson.Number == "" {
			logrus.Warnf("header notifier get empty block. id:%d", ds.id)
			time.Sleep(5000 * time.Millisecond)
			continue
		}

		height := hexutil.MustDecodeUint64(blkJson.Number)
		blockHash := blkJson.Hash
		weight := uint64(0)
		// if blkJson.TotalDifficulty != "" {
		// 	weight = hexutil.MustDecodeUint64(blkJson.TotalDifficulty)
		// }

		ds.remote.Update(height, blockHash)

		ds.remoteChainUpdateChannel <- &types.RemoteChainUpdate{
			NodeId:    ds.id,
			Height:    height,
			BlockHash: blockHash,
			Weight:    weight,
		}
	}
}
