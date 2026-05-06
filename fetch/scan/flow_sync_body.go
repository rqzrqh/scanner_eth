package scan

import (
	"context"
	"strings"
	"time"

	fetchserialstore "scanner_eth/fetch/serial_store"
	fetchstore "scanner_eth/fetch/store"
	fetchtask "scanner_eth/fetch/taskpool"
)

func (sf *Flow) RunSyncBodyStage(ctx context.Context) []fetchserialstore.Branch {
	startedAt := time.Now()
	if !sf.canRunScanStage(ctx) {
		if sf != nil {
			sf.logScanStageEvent(scanStageEvent{stage: scanStageSyncBody, success: false, duration: time.Since(startedAt), errMsg: "scan stage unavailable"})
		}
		return nil
	}
	bodyBranches := sf.collectStoreBranchesForBodySync()
	sf.requestMissingBodySync(bodyBranches)
	sf.logScanStageEvent(scanStageEvent{
		stage:       scanStageSyncBody,
		target:      strings.Join(sf.serializeStoreBranches(bodyBranches), bodyTargetBranchSep),
		targetCount: countBranchNodes(bodyBranches),
		success:     true,
		duration:    time.Since(startedAt),
	})
	return bodyBranches
}

func countBranchNodes(branches []fetchserialstore.Branch) int {
	count := 0
	for _, branch := range branches {
		count += len(branch.Nodes)
	}
	return count
}

func (sf *Flow) EnqueueMissingBodyTasks(branches []fetchserialstore.Branch) {
	sf.requestMissingBodySync(branches)
}

func (sf *Flow) collectStoreBranchesForBodySync() []fetchserialstore.Branch {
	return sf.collectStoreBranchSuffixes()
}

func (sf *Flow) requestMissingBodySync(branches []fetchserialstore.Branch) {
	if sf == nil || sf.taskPool == nil {
		return
	}
	for _, branch := range branches {
		for _, node := range branch.Nodes {
			hash := sf.normalize(node.Hash)
			if hash == "" || node.BlockData != nil {
				continue
			}
			sf.taskPool.EnqueueTaskWithPriority(hash, fetchtask.TaskPriorityHigh)
		}
	}
}

func (sf *Flow) HasStorableNodeData(nodeData *fetchstore.EventBlockData) bool {
	return nodeData != nil && nodeData.StorageFullBlock != nil
}
