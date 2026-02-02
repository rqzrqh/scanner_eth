package publish

import (
	"context"
	"encoding/json"
	"os"
	"sync_eth/protocol"
	"sync_eth/types"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type PublishManager struct {
	w                               *kafka.Writer
	publishOperationChannel         <-chan *types.PublishOperation
	publishFeedbackOperationChannel chan<- *types.PublishFeedbackOperation
}

func NewPublishManager(w *kafka.Writer, publishOperationChannel <-chan *types.PublishOperation, publishFeedbackOperationChannel chan<- *types.PublishFeedbackOperation) *PublishManager {
	return &PublishManager{
		w:                               w,
		publishOperationChannel:         publishOperationChannel,
		publishFeedbackOperationChannel: publishFeedbackOperationChannel,
	}
}

func (pm *PublishManager) Run() {
	go func() {
		for {
			for op := range pm.publishOperationChannel {
				id := uint64(0)
				height := uint64(0)

				switch op.Type {
				case types.PublishApply:

					id = op.Id
					height = op.Height
					protocolFullBlock := op.ProtocolFullBlock

					sannerData := protocol.ScannerData{
						ActionType: protocol.ChainActionApply,
						Height:     height,
						FullBlock:  protocolFullBlock,
					}

					var protocolData []byte
					var err error
					if protocolData, err = json.Marshal(sannerData); err != nil {
						logrus.Errorf("marshal protocol scanner data(apply) failed. height:%v err:%v", height, err)
						os.Exit(0)
					}

					tryCount := 0
					for {
						tryCount++
						startTime := time.Now()

						if err := pm.w.WriteMessages(context.Background(),
							kafka.Message{
								Value: protocolData,
							},
						); err != nil {
							logrus.Errorf("failed to write messages. wait retry. height:%v err:%v tryCount:%v", height, err, tryCount)
							time.Sleep(3 * time.Second)
							continue
						}

						logrus.Infof("publish success. height:%v id:%v cost:%v", height, id, time.Since(startTime).String())
						break
					}

				case types.PublishRollback:
					id = op.Id
					height = op.Height

					sannerData := protocol.ScannerData{
						ActionType: protocol.ChainActionRollback,
						Height:     height,
					}

					var protocolData []byte
					var err error
					if protocolData, err = json.Marshal(sannerData); err != nil {
						logrus.Errorf("marshal protocol scanner data(rollback) failed. height:%v err:%v", height, err)
						os.Exit(0)
					}

					tryCount := 0
					for {
						tryCount++
						startTime := time.Now()

						if err := pm.w.WriteMessages(context.Background(),
							kafka.Message{
								Value: protocolData,
							},
						); err != nil {
							logrus.Errorf("failed to write messages. wait retry. height:%v err:%v tryCount:%v", height, err, tryCount)
							time.Sleep(3 * time.Second)
							continue
						}

						logrus.Infof("publish success. height:%v id:%v cost:%v", height, id, time.Since(startTime).String())
						break
					}

				default:
					logrus.Errorf("unknown publish operation type. type:%v", op.Type)
					os.Exit(0)
				}

				publishFeedbackOperation := &types.PublishFeedbackOperation{
					Id:     id,
					Height: height,
				}

				pm.publishFeedbackOperationChannel <- publishFeedbackOperation
			}
		}
	}()
}
