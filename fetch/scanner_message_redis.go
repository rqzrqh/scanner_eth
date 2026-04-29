package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"scanner_eth/model"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	defaultScannerMessageRedisBatch  = 64
	redisMessageFlushTickerInterval = 200 * time.Millisecond
)

func (fm *FetchManager) scannerMessageRedisBatchSize() int {
	if fm == nil || fm.scannerMsgRedisBatch <= 0 {
		return defaultScannerMessageRedisBatch
	}
	return fm.scannerMsgRedisBatch
}

// flushScannerMessageRedisBatch loads unpushed scanner_message rows, writes them to Redis in batch, then marks pushed.
func (fm *FetchManager) flushScannerMessageRedisBatch(ctx context.Context) (int, error) {
	if fm == nil || fm.db == nil || fm.redisClient == nil {
		return 0, nil
	}

	batch := fm.scannerMessageRedisBatchSize()
	var rows []model.ScannerMessage
	// Select only columns needed for push; idx_scanner_msg_pending(pushed,id) covers WHERE + ORDER BY; avoid SELECT * and extra lookups.
	if err := fm.db.WithContext(ctx).Model(&model.ScannerMessage{}).
		Select("id", "height", "hash", "parent_hash").
		Where("pushed = ?", false).
		Order("id ASC").
		Limit(batch).
		Find(&rows).Error; err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	pipe := fm.redisClient.Pipeline()
	for _, r := range rows {
		key := fm.scannerMessageRedisKey(r.Id)
		payload, err := json.Marshal(struct {
			Height     uint64 `json:"height"`
			Hash       string `json:"hash"`
			ParentHash string `json:"parent_hash"`
		}{
			Height:     r.Height,
			Hash:       r.Hash,
			ParentHash: r.ParentHash,
		})
		if err != nil {
			return 0, err
		}
		pipe.Set(ctx, key, payload, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}

	ids := make([]uint64, len(rows))
	for i := range rows {
		ids[i] = rows[i].Id
	}
	res := fm.db.WithContext(ctx).Model(&model.ScannerMessage{}).
		Where("id IN ? AND pushed = ?", ids, false).
		Update("pushed", true)
	if res.Error != nil {
		return 0, res.Error
	}
	return len(rows), nil
}

// scannerMessageRedisKey format: "<chainName>:scanner:<messageID>" (e.g. bsc_testnet:scanner:0).
func (fm *FetchManager) scannerMessageRedisKey(messageID uint64) string {
	prefix := strings.TrimSpace(fm.chainName)
	if prefix == "" {
		prefix = fmt.Sprintf("chain_%d", fm.chainId)
	}
	return fmt.Sprintf("%s:scanner:%d", prefix, messageID)
}

func (fm *FetchManager) startRedisMessagePushLoop() {
	fm.stopRedisMessagePushLoop()
	if fm == nil || fm.redisClient == nil {
		return
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	fm.redisMessageLoopCancel = cancel
	go func() {
		ticker := time.NewTicker(redisMessageFlushTickerInterval)
		defer ticker.Stop()

		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
			redisAgain:
				n, err := fm.flushScannerMessageRedisBatch(loopCtx)
				if err != nil {
					logrus.Errorf("flushScannerMessageRedisBatch error: %v", err)
					continue
				}
				if n > 0 {
					goto redisAgain
				}
			}
		}
	}()
}

func (fm *FetchManager) stopRedisMessagePushLoop() {
	if fm == nil {
		return
	}
	if fm.redisMessageLoopCancel != nil {
		fm.redisMessageLoopCancel()
		fm.redisMessageLoopCancel = nil
	}
}
