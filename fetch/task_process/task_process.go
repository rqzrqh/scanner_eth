package taskprocess

import (
	"context"
	"scanner_eth/util"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

func HandleBodyTask(deps RuntimeDeps, hash string, stopCh <-chan struct{}) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		select {
		case <-stopCh:
			cancel()
		case <-done:
		}
	}()
	defer close(done)
	return deps.SyncNodeDataByHash(ctx, hash)
}

func HandleHeaderHashTask(deps RuntimeDeps, hash string) bool {
	startedAt := time.Now()
	hash = util.NormalizeHash(hash)
	if hash == "" {
		logrus.Warnf("scan stage event stage:header_by_hash target:%v success:false duration:%v err:%v", hash, time.Since(startedAt), "header-by-hash target is empty")
		return false
	}
	if !deps.SyncHeaderByHash(context.Background(), hash) {
		logrus.Warnf("scan stage event stage:header_by_hash target:%v success:false duration:%v err:%v", hash, time.Since(startedAt), "header-by-hash fetch failed")
		return false
	}
	logrus.Infof("scan stage event stage:header_by_hash target:%v success:true duration:%v", hash, time.Since(startedAt))
	return true
}

func HandleHeaderHeightTask(deps RuntimeDeps, height uint64) bool {
	startedAt := time.Now()
	if deps.SyncHeaderByHeight(context.Background(), height) == nil {
		logrus.Warnf("scan stage event stage:header_by_height target:%v success:false duration:%v err:%v", strconv.FormatUint(height, 10), time.Since(startedAt), "header-by-height fetch failed")
		return false
	}
	logrus.Infof("scan stage event stage:header_by_height target:%v success:true duration:%v", strconv.FormatUint(height, 10), time.Since(startedAt))
	return true
}
