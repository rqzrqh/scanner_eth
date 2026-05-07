package node

import (
	"fmt"
	headernotify "scanner_eth/fetch/header_notify"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/sirupsen/logrus"
)

const (
	nodeScoreFailurePenaltyMicros     int64 = 5_000_000
	nodeScoreInflightPenaltyMicros    int64 = 500_000
	nodeScoreColdStartDelayMicros     int64 = 100_000
	nodeScoreBalanceToleranceMicros   int64 = 250_000
	nodeScoreMaxCooldown                    = time.Minute
	nodeScoreBaseCooldown                   = 5 * time.Second
	nodeScoreEWMANewSampleWeightParts int64 = 3
	nodeScoreEWMAWeightDenominator    int64 = 10
)

type NodeState struct {
	operator            *NodeOperatorImpl
	remote              *headernotify.RemoteChain
	delay               int64
	ready               bool
	disabled            bool
	disabledReason      string
	ewmaDelay           int64
	score               int64
	successCount        uint64
	failureCount        uint64
	consecutiveFailures int
	lastSuccessAt       time.Time
	lastFailureAt       time.Time
	cooldownUntil       time.Time
	inflight            int
}

type NodeSnapshot struct {
	ID                  int    `json:"id"`
	Ready               bool   `json:"ready"`
	Disabled            bool   `json:"disabled"`
	DisabledReason      string `json:"disabled_reason"`
	Delay               int64  `json:"delay"`
	EWMADelay           int64  `json:"ewma_delay"`
	Score               int64  `json:"score"`
	SuccessCount        uint64 `json:"success_count"`
	FailureCount        uint64 `json:"failure_count"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
	Inflight            int    `json:"inflight"`
	CooldownUntilUnix   int64  `json:"cooldown_until_unix"`
	LastSuccessUnix     int64  `json:"last_success_unix"`
	LastFailureUnix     int64  `json:"last_failure_unix"`
	RemoteHeight        uint64 `json:"remote_height"`
	RemoteHash          string `json:"remote_hash"`
}

type ManagerSnapshot struct {
	NodeCount    uint64         `json:"node_count"`
	ReadyCount   uint64         `json:"ready_count"`
	LatestHeight uint64         `json:"latest_height"`
	Nodes        []NodeSnapshot `json:"nodes"`
}

func (n *NodeState) GetChainInfo() uint64 {
	if n == nil || n.remote == nil {
		return 0
	}
	height, _ := n.remote.GetChainInfo()
	return height
}

func (n *NodeState) effectiveReady(now time.Time) bool {
	if n == nil {
		return false
	}
	if n.disabled {
		return false
	}
	if n.ready {
		return true
	}
	return !n.cooldownUntil.IsZero() && !now.Before(n.cooldownUntil)
}

func (n *NodeState) currentScore(now time.Time) int64 {
	if n == nil {
		return 0
	}
	if n.disabled {
		return nodeScoreFailurePenaltyMicros * 100
	}
	delay := n.ewmaDelay
	if delay <= 0 {
		delay = n.delay
	}
	if delay <= 0 {
		delay = nodeScoreColdStartDelayMicros
	}
	score := delay
	score += int64(n.consecutiveFailures) * nodeScoreFailurePenaltyMicros
	score += int64(n.inflight) * nodeScoreInflightPenaltyMicros
	if !n.cooldownUntil.IsZero() && now.Before(n.cooldownUntil) {
		score += nodeScoreFailurePenaltyMicros
	}
	return score
}

type NodeManager struct {
	mu              sync.RWMutex
	nodes           []*NodeState
	selectionCursor uint64
}

func NewNodeManager(clients []*ethclient.Client, rpcTimeout time.Duration) *NodeManager {
	nodes := make([]*NodeState, len(clients))
	for i, client := range clients {
		nodes[i] = &NodeState{
			operator: NewNodeOperator(i, client, rpcTimeout),
			remote:   headernotify.NewRemoteChain(),
			delay:    0,
			score:    nodeScoreColdStartDelayMicros,
			ready:    true,
		}
	}
	return &NodeManager{nodes: nodes}
}

// validNodeLocked reports whether id is in range and nm.nodes[id] is non-nil.
// Caller must hold nm.mu.
func (nm *NodeManager) validNodeLocked(id int) bool {
	return nm != nil && id >= 0 && id < len(nm.nodes) && nm.nodes[id] != nil
}

func (nm *NodeManager) NodeCount() int {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return len(nm.nodes)
}

func (nm *NodeManager) NodeOperators() []NodeOperator {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	ops := make([]NodeOperator, len(nm.nodes))
	for i, node := range nm.nodes {
		if node == nil {
			continue
		}
		ops[i] = node.operator
	}
	return ops
}

type nodeCandidate struct {
	id    int
	node  *NodeState
	score int64
}

type nodeSelectionStats struct {
	nilNodes      int
	disabled      int
	notReady      int
	cooldown      int
	remoteUnknown int
	heightTooLow  int
}

func (nm *NodeManager) validCandidatesLocked(height uint64, now time.Time) ([]nodeCandidate, nodeSelectionStats) {
	stats := nodeSelectionStats{}
	if nm == nil {
		return nil, stats
	}
	nodes := make([]nodeCandidate, 0, len(nm.nodes))
	for id, node := range nm.nodes {
		if node == nil {
			stats.nilNodes++
			continue
		}
		if node.disabled {
			stats.disabled++
			continue
		}
		if !node.cooldownUntil.IsZero() && now.Before(node.cooldownUntil) {
			stats.cooldown++
			continue
		}
		if !node.effectiveReady(now) {
			stats.notReady++
			continue
		}
		if node.remote == nil {
			stats.remoteUnknown++
			continue
		}
		remoteHeight, _ := node.remote.GetChainInfo()
		if remoteHeight < height {
			stats.heightTooLow++
			continue
		}
		score := node.currentScore(now)
		nodes = append(nodes, nodeCandidate{id: id, node: node, score: score})
	}
	sort.SliceStable(nodes, func(i, j int) bool {
		if nodes[i].score == nodes[j].score {
			return nodes[i].id < nodes[j].id
		}
		return nodes[i].score < nodes[j].score
	})
	return nodes, stats
}

func (nm *NodeManager) balancedCandidatesLocked(candidates []nodeCandidate) []nodeCandidate {
	if len(candidates) <= 1 {
		return candidates
	}
	bestScore := candidates[0].score
	eligibleCount := 0
	for _, candidate := range candidates {
		if candidate.score > bestScore+nodeScoreBalanceToleranceMicros {
			break
		}
		eligibleCount++
	}
	if eligibleCount <= 1 {
		return candidates
	}
	shift := int(nm.selectionCursor % uint64(eligibleCount))
	nm.selectionCursor++
	if shift == 0 {
		return candidates
	}
	ordered := make([]nodeCandidate, 0, len(candidates))
	ordered = append(ordered, candidates[shift:eligibleCount]...)
	ordered = append(ordered, candidates[:shift]...)
	ordered = append(ordered, candidates[eligibleCount:]...)
	return ordered
}

func (nm *NodeManager) GetAllValidNodeOperators(height uint64, blockHash string) []NodeOperator {
	if nm == nil {
		return nil
	}
	if strings.TrimSpace(blockHash) == "" {
		return nil
	}
	nm.mu.Lock()
	defer nm.mu.Unlock()
	candidates, stats := nm.validCandidatesLocked(height, time.Now())
	candidates = nm.balancedCandidatesLocked(candidates)
	ops := make([]NodeOperator, 0, len(candidates))
	nodeIDs := make([]int, 0, len(candidates))
	scores := make([]int64, 0, len(candidates))
	for _, candidate := range candidates {
		ops = append(ops, candidate.node.operator)
		if candidate.node.operator != nil {
			nodeIDs = append(nodeIDs, candidate.node.operator.ID())
			scores = append(scores, candidate.score)
		}
	}
	logrus.Debugf("valid node operators selected. height:%v hash:%v valid_nodes:%v node_ids:%v scores:%v disabled:%v not_ready:%v cooldown:%v height_too_low:%v remote_unknown:%v nil_nodes:%v",
		height, strings.TrimSpace(blockHash), len(ops), nodeIDs, scores, stats.disabled, stats.notReady, stats.cooldown, stats.heightTooLow, stats.remoteUnknown, stats.nilNodes)
	return ops
}

func (nm *NodeManager) EthClients() []*ethclient.Client {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	clients := make([]*ethclient.Client, len(nm.nodes))
	for i, node := range nm.nodes {
		if node == nil || node.operator == nil {
			continue
		}
		clients[i] = node.operator.EthClient()
	}
	return clients
}

func (nm *NodeManager) Node(id int) *NodeState {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	if id < 0 || id >= len(nm.nodes) {
		return nil
	}
	return nm.nodes[id]
}

func (nm *NodeManager) ResetNodeRemoteTip(id int) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if !nm.validNodeLocked(id) {
		return
	}
	nm.nodes[id].remote = headernotify.NewRemoteChain()
}

func (nm *NodeManager) UpdateNodeChainInfo(id int, height uint64, hash string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if !nm.validNodeLocked(id) {
		return
	}
	nm.nodes[id].remote.Update(height, hash)
	if !nm.nodes[id].cooldownUntil.After(time.Now()) {
		nm.nodes[id].ready = true
	}
}

func (nm *NodeManager) UpdateNodeState(id int, delay int64, success bool) {
	nm.RecordNodeResult(id, delay, success)
}

func (nm *NodeManager) MarkNodeUnavailable(id int, reason string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if !nm.validNodeLocked(id) {
		return
	}
	node := nm.nodes[id]
	node.disabled = true
	node.disabledReason = strings.TrimSpace(reason)
	node.ready = false
	node.failureCount++
	node.consecutiveFailures++
	node.lastFailureAt = time.Now()
	node.cooldownUntil = time.Time{}
	node.score = node.currentScore(node.lastFailureAt)
}

func (nm *NodeManager) RecordNodeResult(id int, delay int64, success bool) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if !nm.validNodeLocked(id) {
		return
	}
	node := nm.nodes[id]
	if node.inflight > 0 {
		node.inflight--
	}
	if delay < 0 {
		delay = 0
	}
	node.delay = delay
	now := time.Now()
	if success {
		node.successCount++
		node.consecutiveFailures = 0
		node.lastSuccessAt = now
		node.cooldownUntil = time.Time{}
		node.ready = true
		if delay > 0 {
			if node.ewmaDelay <= 0 {
				node.ewmaDelay = delay
			} else {
				oldWeight := nodeScoreEWMAWeightDenominator - nodeScoreEWMANewSampleWeightParts
				node.ewmaDelay = (node.ewmaDelay*oldWeight + delay*nodeScoreEWMANewSampleWeightParts) / nodeScoreEWMAWeightDenominator
			}
		}
	} else {
		node.failureCount++
		node.consecutiveFailures++
		node.lastFailureAt = now
		node.ready = false
		node.cooldownUntil = now.Add(nodeCooldownDuration(node.consecutiveFailures))
		if node.ewmaDelay <= 0 && delay > 0 {
			node.ewmaDelay = delay
		}
	}
	node.score = node.currentScore(now)
}

func nodeCooldownDuration(consecutiveFailures int) time.Duration {
	if consecutiveFailures <= 0 {
		return 0
	}
	d := nodeScoreBaseCooldown
	for i := 1; i < consecutiveFailures; i++ {
		d *= 2
		if d >= nodeScoreMaxCooldown {
			return nodeScoreMaxCooldown
		}
	}
	return d
}

func (nm *NodeManager) SetAllNodesIdle() {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	for _, node := range nm.nodes {
		if node == nil {
			continue
		}
		node.delay = 0
		node.ewmaDelay = 0
		node.score = nodeScoreColdStartDelayMicros
		node.ready = true
		node.disabled = false
		node.disabledReason = ""
		node.consecutiveFailures = 0
		node.cooldownUntil = time.Time{}
		node.inflight = 0
	}
}

func (nm *NodeManager) ResetRemoteChainTips() {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	for _, node := range nm.nodes {
		if node == nil {
			continue
		}
		node.remote = headernotify.NewRemoteChain()
	}
}

func (nm *NodeManager) GetLatestHeight() uint64 {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	var latest uint64
	for _, node := range nm.nodes {
		if node == nil || node.remote == nil {
			continue
		}
		if h, _ := node.remote.GetChainInfo(); h > latest {
			latest = h
		}
	}
	return latest
}

func (nm *NodeManager) GetBestNode(height uint64) (int, NodeOperator, error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	candidates, _ := nm.validCandidatesLocked(height, time.Now())
	candidates = nm.balancedCandidatesLocked(candidates)
	if len(candidates) == 0 {
		return -1, nil, fmt.Errorf("no valid node with height >= %d", height)
	}
	best := candidates[0]
	best.node.inflight++
	best.node.score = best.node.currentScore(time.Now())
	return best.id, best.node.operator, nil
}

func (nm *NodeManager) Snapshot() ManagerSnapshot {
	if nm == nil {
		return ManagerSnapshot{Nodes: []NodeSnapshot{}}
	}
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	snapshot := ManagerSnapshot{
		NodeCount: uint64(len(nm.nodes)),
		Nodes:     make([]NodeSnapshot, 0, len(nm.nodes)),
	}
	for id, node := range nm.nodes {
		if node == nil {
			snapshot.Nodes = append(snapshot.Nodes, NodeSnapshot{ID: id})
			continue
		}
		now := time.Now()
		var remoteHeight uint64
		var remoteHash string
		if node.remote != nil {
			remoteHeight, remoteHash = node.remote.GetChainInfo()
		}
		ready := node.effectiveReady(now)
		score := node.currentScore(now)
		if ready {
			snapshot.ReadyCount++
		}
		if remoteHeight > snapshot.LatestHeight {
			snapshot.LatestHeight = remoteHeight
		}
		cooldownUntil := int64(0)
		if !node.cooldownUntil.IsZero() {
			cooldownUntil = node.cooldownUntil.Unix()
		}
		lastSuccess := int64(0)
		if !node.lastSuccessAt.IsZero() {
			lastSuccess = node.lastSuccessAt.Unix()
		}
		lastFailure := int64(0)
		if !node.lastFailureAt.IsZero() {
			lastFailure = node.lastFailureAt.Unix()
		}
		snapshot.Nodes = append(snapshot.Nodes, NodeSnapshot{
			ID:                  id,
			Ready:               ready,
			Disabled:            node.disabled,
			DisabledReason:      node.disabledReason,
			Delay:               node.delay,
			EWMADelay:           node.ewmaDelay,
			Score:               score,
			SuccessCount:        node.successCount,
			FailureCount:        node.failureCount,
			ConsecutiveFailures: node.consecutiveFailures,
			Inflight:            node.inflight,
			CooldownUntilUnix:   cooldownUntil,
			LastSuccessUnix:     lastSuccess,
			LastFailureUnix:     lastFailure,
			RemoteHeight:        remoteHeight,
			RemoteHash:          remoteHash,
		})
	}
	return snapshot
}
