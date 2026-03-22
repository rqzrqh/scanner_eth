package fetch

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/sirupsen/logrus"
)

type HeaderNotifier struct {
	id                       int
	client                   *ethclient.Client
	remote                   *RemoteChain
	remoteChainUpdateChannel chan<- *RemoteChainUpdate
}

func NewHeaderNotifier(id int, client *ethclient.Client, remoteChainUpdateChannel chan<- *RemoteChainUpdate) *HeaderNotifier {
	return &HeaderNotifier{
		id:                       id,
		client:                   client,
		remote:                   NewRemoteChain(),
		remoteChainUpdateChannel: remoteChainUpdateChannel,
	}
}

func (ds *HeaderNotifier) Run() {
	if ds == nil || ds.client == nil {
		return
	}

	go func() {

		if ds.client.Client().SupportsSubscriptions() {
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
		sub, err := ds.client.SubscribeNewHead(context.Background(), newHeadChannel)
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

				logrus.Debugf("header notifier new header. id:%d height:%v hash:%v", ds.id, height, blockHash)

				ds.remote.Update(height, blockHash)
				ds.remoteChainUpdateChannel <- &RemoteChainUpdate{
					NodeId:    ds.id,
					Height:    height,
					BlockHash: blockHash,
					Weight:    weight,
					Header:    toRemoteHeader(header),
				}
			}
		}
	}
}

func (ds *HeaderNotifier) useHttp() {
	for {
		header, err := ds.client.HeaderByNumber(context.Background(), nil)
		if err != nil {
			logrus.Warnf("header notifier failed to get latest block. id:%d error:%v", ds.id, err)
			time.Sleep(5000 * time.Millisecond)
			continue
		}
		if header == nil {
			logrus.Warnf("header notifier get empty latest block. id:%d", ds.id)
			time.Sleep(5000 * time.Millisecond)
			continue
		}

		height := header.Number.Uint64()
		blockHash := header.Hash().Hex()
		weight := uint64(0)
		// if header.TotalDifficulty != "" {
		// 	weight = hexutil.MustDecodeUint64(header.TotalDifficulty)
		// }

		ds.remote.Update(height, blockHash)

		ds.remoteChainUpdateChannel <- &RemoteChainUpdate{
			NodeId:    ds.id,
			Height:    height,
			BlockHash: blockHash,
			Weight:    weight,
			Header:    toRemoteHeader(header),
		}

		time.Sleep(3000 * time.Millisecond)
	}
}

func toRemoteHeader(header *ethTypes.Header) *RemoteHeader {
	if header == nil {
		return nil
	}

	difficulty := ""
	if header.Difficulty != nil {
		difficulty = hexutil.EncodeBig(header.Difficulty)
	}

	number := ""
	if header.Number != nil {
		number = hexutil.EncodeBig(header.Number)
	}

	return &RemoteHeader{
		Hash:       header.Hash().Hex(),
		ParentHash: header.ParentHash.Hex(),
		Number:     number,
		Difficulty: difficulty,
	}
}
