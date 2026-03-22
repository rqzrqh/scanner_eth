package publish

import (
	"context"
	"errors"
	"scanner_eth/leader"
	"scanner_eth/model"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type PublishManager struct {
	db        *gorm.DB
	election  *leader.Election
	w         messageWriter
	loopCancel context.CancelFunc
}

func NewPublishManager(chainName string, db *gorm.DB, redisCilent *redis.Client, w *kafka.Writer) *PublishManager {

	election := leader.NewElection(chainName, redisCilent)

	return &PublishManager{
		db:       db,
		election: election,
		w:        w,
	}
}

func (pm *PublishManager) Run() {
	go pm.election.DoWithLeaderElection(context.Background(), "publishEvent", time.Second, pm.onBecameLeader, pm.onLostLeader)
}

func (pm *PublishManager) Stop() {
	if pm == nil {
		return
	}
	pm.stopPublishLoop()
	if pm.election != nil {
		pm.election.TriggerLostLeader()
	}
}

func (pm *PublishManager) onBecameLeader(ctx context.Context) error {
	_ = ctx
	pm.startPublishLoop()
	return nil
}

func (pm *PublishManager) onLostLeader(ctx context.Context) error {
	_ = ctx
	pm.stopPublishLoop()
	return nil
}

func (pm *PublishManager) startPublishLoop() {
	pm.stopPublishLoop()

	loopCtx, cancel := context.WithCancel(context.Background())
	pm.loopCancel = cancel
	loopInterval := time.Second

	go func() {
		ticker := time.NewTicker(loopInterval)
		defer ticker.Stop()

		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
			forAgain:
				again, err := pm.publishEvent(loopCtx)
				if err != nil {
					logrus.Errorf("publishEvent error: %v", err)
					continue
				}
				if again {
					goto forAgain
				}
			}
		}
	}()
}

func (pm *PublishManager) stopPublishLoop() {
	if pm.loopCancel != nil {
		pm.loopCancel()
		pm.loopCancel = nil
	}
}

func (pm *PublishManager) publishEvent(ctx context.Context) (bool, error) {
	// 1. 从 scanner_info 读取 chain_id 与 published_message_id
	var scannerInfo model.ScannerInfo
	if err := pm.db.First(&scannerInfo).Error; err != nil {
		logrus.Warnf("publishEvent get scanner_info failed: %v", err)
		return true, nil
	}
	nextMessageId := scannerInfo.PublishedMessageId + 1

	// 2. 从 chain_binlog 读取下一条（message_id = published_message_id + 1）
	var binlog model.ChainBinlog
	if err := pm.db.Where("message_id = ?", nextMessageId).First(&binlog).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return true, nil
		}
		logrus.Warnf("publishEvent get chain_binlog failed: %v", err)
		return true, nil
	}

	// 3. 推送给远端
	if pm.w == nil {
		return false, errors.New("kafka writer is nil")
	}
	if err := pm.w.WriteMessages(ctx, kafka.Message{Value: binlog.Data}); err != nil {
		logrus.Warnf("publishEvent write kafka failed. message_id:%v err:%v", binlog.MessageId, err)
		return false, err
	}

	// 4. 成功后在事务中：删除该 chain_binlog 记录，并更新 scanner_info.published_message_id，且必须 affectRows == 1
	err := pm.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&model.ChainBinlog{}, binlog.Id).Error; err != nil {
			logrus.Warnf("publishEvent delete chain_binlog failed. id:%v err:%v", binlog.Id, err)
			return err
		}
		result := tx.Model(&model.ScannerInfo{}).
			Where("chain_id = ? AND published_message_id = ?", scannerInfo.ChainId, scannerInfo.PublishedMessageId).
			Update("published_message_id", binlog.MessageId)
		if result.Error != nil {
			logrus.Warnf("publishEvent update scanner_info failed: %v", result.Error)
			return result.Error
		}
		if result.RowsAffected == 0 {
			logrus.Warnf("publishEvent update scanner_info affect rows not 1. affected:%v", result.RowsAffected)
			return errors.New("update scanner_info affected rows is 0, possible concurrent update")
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	logrus.Infof("publishEvent success. message_id:%v height:%v", binlog.MessageId, binlog.Height)
	return true, nil
}
