package headernotify

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/sirupsen/logrus"
)

type HeaderNotifier struct {
	id     int
	client *ethclient.Client
	remote *RemoteChain
}

func NewHeaderNotifier(id int, client *ethclient.Client) *HeaderNotifier {
	return &HeaderNotifier{
		id:     id,
		client: client,
		remote: NewRemoteChain(),
	}
}

// Run starts a goroutine that publishes new heads to out until ctx is cancelled.
// wg is optional; when non-nil, Run calls wg.Add(1) before starting and Done in the worker.
func (ds *HeaderNotifier) Run(ctx context.Context, out chan<- *RemoteChainUpdate, wg *sync.WaitGroup) {
	if ds == nil || ds.client == nil || out == nil {
		return
	}
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		if ds.client.Client().SupportsSubscriptions() {
			logrus.Infof("header notifier use websocket. id:%d", ds.id)
			ds.useWebsocket(ctx, out)
		} else {
			logrus.Infof("header notifier use http. id:%d", ds.id)
			ds.useHttp(ctx, out)
		}
	}()
}

func (ds *HeaderNotifier) useWebsocket(ctx context.Context, out chan<- *RemoteChainUpdate) {
	newHeadChannel := make(chan *ethTypes.Header, 5)
	defer close(newHeadChannel)

RECONNECT:
	select {
	case <-ctx.Done():
		return
	default:
	}

	sub, err := ds.client.SubscribeNewHead(ctx, newHeadChannel)
	if err != nil {
		logrus.Warnf("header notifier failed to subscribe newHeads. wait reconnect. id:%d error:%v", ds.id, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(3000 * time.Millisecond):
			goto RECONNECT
		}
	}

	logrus.Infof("header notifier subscribe newHeads success. id:%d", ds.id)

	for {
		select {
		case <-ctx.Done():
			sub.Unsubscribe()
			return
		case err := <-sub.Err():
			logrus.Warnf("header notifier newHeads subscription error. id:%d error:%v", ds.id, err)
			sub.Unsubscribe()
			goto RECONNECT
		case header := <-newHeadChannel:
			if header == nil {
				logrus.Warnf("header notifier newHeads get nil header. wait reconnect. id:%d", ds.id)
				sub.Unsubscribe()
				select {
				case <-ctx.Done():
					return
				case <-time.After(3000 * time.Millisecond):
					goto RECONNECT
				}
			}

			height := header.Number.Uint64()
			blockHash := header.Hash().Hex()
			weight := uint64(0)

			logrus.Debugf("header notifier new header. id:%d height:%v hash:%v", ds.id, height, blockHash)

			ds.remote.Update(height, blockHash)
			update := &RemoteChainUpdate{
				NodeId:    ds.id,
				Height:    height,
				BlockHash: blockHash,
				Weight:    weight,
				Header:    toRemoteHeader(header),
			}
			select {
			case out <- update:
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			}
		}
	}
}

func (ds *HeaderNotifier) useHttp(ctx context.Context, out chan<- *RemoteChainUpdate) {
	ticker := time.NewTicker(3000 * time.Millisecond)
	defer ticker.Stop()

	for {
		header, err := ds.client.HeaderByNumber(ctx, nil)
		if err != nil {
			logrus.Warnf("header notifier failed to get latest block. id:%d error:%v", ds.id, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5000 * time.Millisecond):
			}
			continue
		}
		if header == nil {
			logrus.Warnf("header notifier get empty latest block. id:%d", ds.id)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5000 * time.Millisecond):
			}
			continue
		}

		height := header.Number.Uint64()
		blockHash := header.Hash().Hex()
		weight := uint64(0)

		ds.remote.Update(height, blockHash)

		update := &RemoteChainUpdate{
			NodeId:    ds.id,
			Height:    height,
			BlockHash: blockHash,
			Weight:    weight,
			Header:    toRemoteHeader(header),
		}
		select {
		case out <- update:
		case <-ctx.Done():
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
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
