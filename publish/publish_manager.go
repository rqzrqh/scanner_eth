package publish

import (
	"context"
	"scanner_eth/types"
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
				binlogRecordId := op.BinlogRecordId
				messageId := op.MessageId
				height := op.Height
				binlogData := op.BinlogData

				tryCount := 0
				for {
					tryCount++
					startTime := time.Now()

					if err := pm.w.WriteMessages(context.Background(),
						kafka.Message{
							Value: binlogData,
						},
					); err != nil {
						logrus.Errorf("failed to write messages. wait retry. binlog_record_id:%v message_id:%v height:%v err:%v tryCount:%v", binlogRecordId, messageId, height, err, tryCount)
						time.Sleep(3 * time.Second)
						continue
					}

					logrus.Infof("publish success. binlog_record_id:%v message_id:%v height:%v cost:%v", binlogRecordId, messageId, height, time.Since(startTime).String())
					break
				}

				publishFeedbackOperation := &types.PublishFeedbackOperation{
					BinlogRecordId: binlogRecordId,
					MessageId:      messageId,
					Height:         height,
				}

				pm.publishFeedbackOperationChannel <- publishFeedbackOperation
			}
		}
	}()
}
