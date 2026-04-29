package scan

import (
	"context"
	fetchtask "scanner_eth/fetch/task"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

func HandleTaskPoolTask(flow *Flow, task *fetchtask.SyncTask, stopCh <-chan struct{}) bool {
	if flow == nil || task == nil {
		return task == nil
	}
	flow.BindRuntimeDeps()

	select {
	case <-stopCh:
		return false
	default:
	}

	if task.Kind == fetchtask.SyncTaskKindBody || (task.Kind == 0 && task.Hash != "") {
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
		success := flow.SyncNodeDataByHash(ctx, task.Hash)
		close(done)
		if success {
			flow.TriggerScan()
		}
		return success
	}

	startedAt := time.Now()
	if task.Kind == fetchtask.SyncTaskKindHeaderHash {
		success := flow.FetchAndInsertHeaderByHashCore(task.Hash)
		if !success {
			logrus.Warnf("scan stage event stage:header_by_hash target:%v success:false duration:%v err:%v", flow.normalize(task.Hash), time.Since(startedAt), "header-by-hash fetch failed")
			return false
		}
		logrus.Infof("scan stage event stage:header_by_hash target:%v success:true duration:%v", flow.normalize(task.Hash), time.Since(startedAt))
		flow.TriggerScan()
		return true
	}

	header := flow.FetchAndInsertHeaderByHeightCore(task.Height)
	if header == nil {
		logrus.Warnf("scan stage event stage:header_by_height target:%v success:false duration:%v err:%v", strconv.FormatUint(task.Height, 10), time.Since(startedAt), "header-by-height fetch failed")
		return false
	}
	logrus.Infof("scan stage event stage:header_by_height target:%v success:true duration:%v", strconv.FormatUint(task.Height, 10), time.Since(startedAt))
	flow.TriggerScan()
	return true
}
