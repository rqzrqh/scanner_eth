package scan

import (
	"context"
	"fmt"
	"strings"
	"time"

	fetchserialstore "scanner_eth/fetch/serial_store"
)

func (sf *Flow) RunStoreBranchesStage(ctx context.Context, bodyBranches []fetchserialstore.Branch) {
	if !sf.canRunScanStage(ctx) || len(bodyBranches) == 0 {
		return
	}
	sf.waitStoreBranchesWrite(ctx, bodyBranches)
}

func (sf *Flow) GetStoreBranchTargets() []string {
	return sf.serializeStoreBranches(sf.collectStoreBranchesForWrite())
}

func (sf *Flow) GetBodyBranchTargets() []string {
	return sf.serializeStoreBranches(sf.collectStoreBranchesForBodySync())
}

func (sf *Flow) BuildStoreBranches() []fetchserialstore.Branch {
	return sf.collectStoreBranchesForWrite()
}

func (sf *Flow) collectStoreBranchSuffixes() []fetchserialstore.Branch {
	if sf == nil {
		return nil
	}
	branches := make([]fetchserialstore.Branch, 0)
	for _, branch := range sf.blockTree.Branches() {
		nodes := make([]fetchserialstore.BranchNode, 0, len(branch.Nodes))
		started := false
		for i := len(branch.Nodes) - 1; i >= 0; i-- {
			node := branch.Nodes[i]
			if node == nil {
				continue
			}
			hash := sf.normalize(node.Key)
			if hash == "" {
				continue
			}
			if !started && sf.storedBlocks != nil && sf.storedBlocks.IsStored(hash) {
				continue
			}
			started = true
			nodes = append(nodes, fetchserialstore.BranchNode{
				Hash:       hash,
				ParentHash: sf.normalize(node.ParentKey),
				Height:     node.Height,
				BlockData:  sf.getPendingBody(node.Key),
			})
		}
		if len(nodes) == 0 {
			continue
		}
		branches = append(branches, fetchserialstore.Branch{Nodes: nodes})
	}
	return branches
}

func (sf *Flow) collectStoreBranchesForWrite() []fetchserialstore.Branch {
	return sf.filterStorableBranches(sf.collectStoreBranchSuffixes())
}

func (sf *Flow) filterStorableBranches(branches []fetchserialstore.Branch) []fetchserialstore.Branch {
	if sf == nil || len(branches) == 0 {
		return nil
	}
	filtered := make([]fetchserialstore.Branch, 0, len(branches))
	for _, branch := range branches {
		nodes := sf.filterStorableBranchNodes(branch.Nodes)
		if len(nodes) == 0 {
			continue
		}
		filtered = append(filtered, fetchserialstore.Branch{Nodes: nodes})
	}
	return filtered
}

func (sf *Flow) filterStorableBranchNodes(nodes []fetchserialstore.BranchNode) []fetchserialstore.BranchNode {
	if sf == nil || len(nodes) == 0 {
		return nil
	}
	filtered := make([]fetchserialstore.BranchNode, 0, len(nodes))
	readyParents := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		hash := sf.normalize(node.Hash)
		if hash == "" {
			break
		}
		if sf.storedBlocks != nil && sf.storedBlocks.IsStored(hash) {
			readyParents[hash] = struct{}{}
			continue
		}
		parentHash := sf.normalize(node.ParentHash)
		parentReady := parentHash == ""
		if !parentReady {
			_, parentReady = readyParents[parentHash]
		}
		if !parentReady && sf.storedBlocks != nil {
			parentReady = sf.storedBlocks.IsStored(parentHash)
		}
		if !parentReady || !sf.HasStorableNodeData(node.BlockData) {
			break
		}
		filtered = append(filtered, node)
		readyParents[hash] = struct{}{}
	}
	return filtered
}

func (sf *Flow) serializeStoreBranches(branches []fetchserialstore.Branch) []string {
	if sf == nil || len(branches) == 0 {
		return nil
	}
	targets := make([]string, 0, len(branches))
	for _, branch := range branches {
		hashes := make([]string, 0, len(branch.Nodes))
		for _, node := range branch.Nodes {
			hash := sf.normalize(node.Hash)
			if hash == "" {
				continue
			}
			hashes = append(hashes, hash)
		}
		if len(hashes) == 0 {
			continue
		}
		targets = append(targets, strings.Join(hashes, bodyTargetNodeSep))
	}
	return targets
}

func (sf *Flow) buildStoreBranchesFromTarget(branchTarget string) []fetchserialstore.Branch {
	if sf == nil || strings.TrimSpace(branchTarget) == "" {
		return nil
	}
	branches := make([]fetchserialstore.Branch, 0)
	for _, rawBranch := range strings.Split(branchTarget, bodyTargetBranchSep) {
		rawBranchTarget := strings.TrimSpace(rawBranch)
		if rawBranchTarget == "" {
			continue
		}
		nodes := make([]fetchserialstore.BranchNode, 0)
		for _, raw := range strings.Split(rawBranchTarget, bodyTargetNodeSep) {
			hash := sf.normalize(raw)
			if hash == "" {
				continue
			}
			node := sf.blockTree.Get(hash)
			if node == nil {
				continue
			}
			nodes = append(nodes, fetchserialstore.BranchNode{
				Hash:       hash,
				ParentHash: sf.normalize(node.ParentKey),
				Height:     node.Height,
				BlockData:  sf.getPendingBody(node.Key),
			})
		}
		if len(nodes) == 0 {
			continue
		}
		branches = append(branches, fetchserialstore.Branch{Nodes: nodes})
	}
	return branches
}

func (sf *Flow) SubmitStoreBranches(ctx context.Context, branches []fetchserialstore.Branch) error {
	if sf == nil {
		return fmt.Errorf("scan runtime or blocktree not available")
	}
	if sf.storeWorker == nil {
		return fmt.Errorf("store block worker is not initialized")
	}
	branches = sf.filterStorableBranches(branches)
	if len(branches) == 0 {
		return nil
	}
	return sf.storeWorker.SubmitBranches(ctx, branches)
}

func (sf *Flow) waitStoreBranchesWrite(ctx context.Context, bodyBranches []fetchserialstore.Branch) {
	if sf == nil || len(bodyBranches) == 0 {
		return
	}
	// Scan keeps missing-body scheduling on the fetch side, then submits only the
	// contiguous storable prefix of each low-to-high branch to serial_store.
	storeBranches := sf.filterStorableBranches(bodyBranches)
	bodyBranchTarget := strings.Join(sf.serializeStoreBranches(storeBranches), bodyTargetBranchSep)
	startedAt := time.Now()
	if ctx != nil && ctx.Err() != nil {
		sf.logScanStageEvent(scanStageEvent{stage: scanStageStoreBranches, target: bodyBranchTarget, targetCount: countBranchNodes(storeBranches), duration: time.Since(startedAt), errMsg: "scan context cancelled"})
		return
	}
	err := sf.SubmitStoreBranches(ctx, storeBranches)
	event := scanStageEvent{stage: scanStageStoreBranches, target: bodyBranchTarget, targetCount: countBranchNodes(storeBranches), success: err == nil, duration: time.Since(startedAt)}
	if err != nil {
		event.errMsg = err.Error()
	}
	sf.logScanStageEvent(event)
	if event.success && (ctx == nil || ctx.Err() == nil) {
		sf.inspectBlockTreeState("after_store_branches")
	}
}

func (sf *Flow) SyncBodyBranchTarget(ctx context.Context, branchTarget string) (bool, string) {
	return sf.SyncStoreBranchTarget(ctx, branchTarget)
}

func (sf *Flow) SyncStoreBranchTarget(ctx context.Context, branchTarget string) (bool, string) {
	if strings.TrimSpace(branchTarget) == "" {
		return false, "invalid body branch target"
	}
	if sf == nil {
		return false, "scan runtime or blocktree not available"
	}
	if err := sf.SubmitStoreBranches(ctx, sf.buildStoreBranchesFromTarget(branchTarget)); err != nil {
		return false, err.Error()
	}
	return true, ""
}

func (sf *Flow) CountActionableBodyNodes() int {
	return sf.CountStoreBranchNodes()
}

func (sf *Flow) CountStoreBranchNodes() int {
	if sf == nil {
		return 0
	}
	count := 0
	for _, branch := range sf.collectStoreBranchesForWrite() {
		count += len(branch.Nodes)
	}
	return count
}

func (sf *Flow) CountStoredLinkedNodes() int {
	return sf.CountStoredLinkedTreeNodes()
}

func (sf *Flow) CountStoredLinkedTreeNodes() int {
	if sf == nil {
		return 0
	}
	count := 0
	for _, node := range sf.blockTree.LinkedNodes() {
		if node != nil && sf.storedBlocks.IsStored(node.Key) {
			count++
		}
	}
	return count
}

func (sf *Flow) ProcessBranchesLowToHigh(ctx context.Context) {
	sf.SubmitStoreBranchesLowToHigh(ctx)
}

func (sf *Flow) SubmitStoreBranchesLowToHigh(ctx context.Context) {
	sf.BindRuntimeDeps()
	if sf == nil {
		return
	}
	// Scan only constructs low-to-high branches from blocktree and hands them to
	// the serial store worker; branch-local stop conditions live in serial_store.
	if sf.storeWorker == nil {
		return
	}
	_ = sf.storeWorker.SubmitBranches(ctx, sf.collectStoreBranchesForWrite())
}
