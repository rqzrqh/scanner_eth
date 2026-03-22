package publish

import (
	"context"
	"errors"
	"fmt"
	"scanner_eth/model"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestPublishManagerStop_NilReceiver(t *testing.T) {
	var pm *PublishManager
	pm.Stop()
}

func TestPublishManagerLeaderCallbacksManageLoop(t *testing.T) {
	pm := &PublishManager{db: newPublishTestDB(t), w: &stubWriter{}}
	if pm.loopCancel != nil {
		t.Fatalf("expected nil loopCancel at start")
	}
	if err := pm.onBecameLeader(context.Background()); err != nil {
		t.Fatalf("onBecameLeader failed: %v", err)
	}
	if pm.loopCancel == nil {
		t.Fatalf("expected loopCancel to be set after onBecameLeader")
	}
	if err := pm.onLostLeader(context.Background()); err != nil {
		t.Fatalf("onLostLeader failed: %v", err)
	}
	if pm.loopCancel != nil {
		t.Fatalf("expected loopCancel to be nil after onLostLeader")
	}
}

type stubWriter struct {
	writeFn func(ctx context.Context, msgs ...kafka.Message) error
	calls   int
}

func (s *stubWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	s.calls++
	if s.writeFn != nil {
		return s.writeFn(ctx, msgs...)
	}
	return nil
}

func newPublishTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite db failed: %v", err)
	}
	if err := db.AutoMigrate(&model.ScannerInfo{}, &model.ChainBinlog{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	return db
}

func TestPublishEvent_NoScannerInfo(t *testing.T) {
	pm := &PublishManager{db: newPublishTestDB(t), w: &stubWriter{}}
	again, err := pm.publishEvent(context.Background())
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !again {
		t.Fatalf("expected again=true when scanner_info is missing")
	}
}

func TestPublishEvent_NoNextBinlog(t *testing.T) {
	db := newPublishTestDB(t)
	if err := db.Create(&model.ScannerInfo{ChainId: 1, PublishedMessageId: 7}).Error; err != nil {
		t.Fatalf("insert scanner info failed: %v", err)
	}
	pm := &PublishManager{db: db, w: &stubWriter{}}
	again, err := pm.publishEvent(context.Background())
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !again {
		t.Fatalf("expected again=true when next binlog is missing")
	}
}

func TestPublishEvent_WriteKafkaFailed(t *testing.T) {
	db := newPublishTestDB(t)
	if err := db.Create(&model.ScannerInfo{ChainId: 1, PublishedMessageId: 0}).Error; err != nil {
		t.Fatalf("insert scanner info failed: %v", err)
	}
	if err := db.Create(&model.ChainBinlog{MessageId: 1, Height: 10, Hash: "0xa", ParentHash: "0x0", Data: []byte("x")}).Error; err != nil {
		t.Fatalf("insert binlog failed: %v", err)
	}
	pm := &PublishManager{db: db, w: &stubWriter{writeFn: func(ctx context.Context, msgs ...kafka.Message) error {
		return errors.New("kafka down")
	}}}

	again, err := pm.publishEvent(context.Background())
	if err == nil {
		t.Fatalf("expected write error")
	}
	if again {
		t.Fatalf("expected again=false on kafka write error")
	}

	var info model.ScannerInfo
	if err := db.First(&info).Error; err != nil {
		t.Fatalf("reload scanner info failed: %v", err)
	}
	if info.PublishedMessageId != 0 {
		t.Fatalf("published_message_id should remain unchanged, got %d", info.PublishedMessageId)
	}
	var cnt int64
	if err := db.Model(&model.ChainBinlog{}).Count(&cnt).Error; err != nil {
		t.Fatalf("count binlog failed: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("binlog should remain for retry, got count=%d", cnt)
	}
}

func TestPublishEvent_NilWriter(t *testing.T) {
	db := newPublishTestDB(t)
	if err := db.Create(&model.ScannerInfo{ChainId: 1, PublishedMessageId: 0}).Error; err != nil {
		t.Fatalf("insert scanner info failed: %v", err)
	}
	if err := db.Create(&model.ChainBinlog{MessageId: 1, Height: 10, Hash: "0xa", ParentHash: "0x0", Data: []byte("x")}).Error; err != nil {
		t.Fatalf("insert binlog failed: %v", err)
	}
	pm := &PublishManager{db: db}
	again, err := pm.publishEvent(context.Background())
	if err == nil {
		t.Fatalf("expected nil writer error")
	}
	if again {
		t.Fatalf("expected again=false when writer is nil")
	}
}

func TestPublishEvent_Success(t *testing.T) {
	db := newPublishTestDB(t)
	if err := db.Create(&model.ScannerInfo{ChainId: 1, PublishedMessageId: 0}).Error; err != nil {
		t.Fatalf("insert scanner info failed: %v", err)
	}
	if err := db.Create(&model.ChainBinlog{MessageId: 1, Height: 10, Hash: "0xa", ParentHash: "0x0", Data: []byte("payload")}).Error; err != nil {
		t.Fatalf("insert binlog failed: %v", err)
	}
	w := &stubWriter{}
	pm := &PublishManager{db: db, w: w}

	again, err := pm.publishEvent(context.Background())
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !again {
		t.Fatalf("expected again=true to continue draining")
	}
	if w.calls != 1 {
		t.Fatalf("expected one kafka write call, got %d", w.calls)
	}

	var info model.ScannerInfo
	if err := db.First(&info).Error; err != nil {
		t.Fatalf("reload scanner info failed: %v", err)
	}
	if info.PublishedMessageId != 1 {
		t.Fatalf("expected published_message_id=1, got %d", info.PublishedMessageId)
	}
	var cnt int64
	if err := db.Model(&model.ChainBinlog{}).Count(&cnt).Error; err != nil {
		t.Fatalf("count binlog failed: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("binlog should be deleted after successful publish, got count=%d", cnt)
	}
}

func TestPublishEvent_UpdateScannerInfoConflict(t *testing.T) {
	db := newPublishTestDB(t)
	if err := db.Create(&model.ScannerInfo{ChainId: 1, PublishedMessageId: 0}).Error; err != nil {
		t.Fatalf("insert scanner info failed: %v", err)
	}
	if err := db.Create(&model.ChainBinlog{MessageId: 1, Height: 10, Hash: "0xa", ParentHash: "0x0", Data: []byte("x")}).Error; err != nil {
		t.Fatalf("insert binlog failed: %v", err)
	}
	pm := &PublishManager{db: db, w: &stubWriter{writeFn: func(ctx context.Context, msgs ...kafka.Message) error {
		return db.Model(&model.ScannerInfo{}).Where("chain_id = ?", 1).Update("published_message_id", 99).Error
	}}}

	again, err := pm.publishEvent(context.Background())
	if err == nil {
		t.Fatalf("expected conflict error")
	}
	if again {
		t.Fatalf("expected again=false on transaction conflict")
	}
	var cnt int64
	if err := db.Model(&model.ChainBinlog{}).Count(&cnt).Error; err != nil {
		t.Fatalf("count binlog failed: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("binlog should remain when transaction fails, got count=%d", cnt)
	}
}