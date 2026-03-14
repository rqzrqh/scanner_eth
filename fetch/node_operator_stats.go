package fetch

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
)

const defaultNodeOperatorStatsInterval = 60 * time.Second

var (
	nodeOperatorStatsMu     sync.Mutex
	nodeOperatorStats       map[int]map[string]int64 // nodeId -> "biz:rpcMethod" or rpcMethod -> count
	nodeOperatorStatsOnce   sync.Once
	nodeOperatorStatsPeriod = defaultNodeOperatorStatsInterval
)

func recordNodeOperatorRPC(nodeID int, method string, delta int64) {
	if delta <= 0 || method == "" {
		return
	}
	nodeOperatorStatsOnce.Do(func() {
		go nodeOperatorStatsReporterLoop(nodeOperatorStatsPeriod)
	})
	nodeOperatorStatsMu.Lock()
	if nodeOperatorStats == nil {
		nodeOperatorStats = make(map[int]map[string]int64)
	}
	byMethod, ok := nodeOperatorStats[nodeID]
	if !ok {
		byMethod = make(map[string]int64)
		nodeOperatorStats[nodeID] = byMethod
	}
	byMethod[method] += delta
	nodeOperatorStatsMu.Unlock()
}

func recordRPCBatchElems(n *NodeOperator, business string, elems []rpc.BatchElem) {
	if n == nil || len(elems) == 0 {
		return
	}
	counts := make(map[string]int64)
	for _, e := range elems {
		if e.Method != "" {
			counts[e.Method]++
		}
	}
	for rpcMethod, c := range counts {
		key := rpcStatKey(business, rpcMethod)
		recordNodeOperatorRPC(n.id, key, c)
	}
}

func rpcStatKey(business, rpcMethod string) string {
	if business == "" {
		return rpcMethod
	}
	return business + ":" + rpcMethod
}

func (n *NodeOperator) recordRPC(business, rpcMethod string, delta int64) {
	if n == nil {
		return
	}
	recordNodeOperatorRPC(n.id, rpcStatKey(business, rpcMethod), delta)
}

func nodeOperatorStatsReporterLoop(every time.Duration) {
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for range t.C {
		logNodeOperatorRPCStats()
	}
}

func logNodeOperatorRPCStats() {
	nodeOperatorStatsMu.Lock()
	if len(nodeOperatorStats) == 0 {
		nodeOperatorStatsMu.Unlock()
		return
	}
	nodeIDs := make([]int, 0, len(nodeOperatorStats))
	for id := range nodeOperatorStats {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Ints(nodeIDs)

	type row struct {
		id  int
		ops map[string]int64
	}
	rows := make([]row, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		src := nodeOperatorStats[id]
		if len(src) == 0 {
			continue
		}
		cp := make(map[string]int64, len(src))
		for k, v := range src {
			cp[k] = v
		}
		rows = append(rows, row{id: id, ops: cp})
	}
	nodeOperatorStatsMu.Unlock()

	for _, r := range rows {
		methods := make([]string, 0, len(r.ops))
		for k := range r.ops {
			methods = append(methods, k)
		}
		sort.Strings(methods)
		parts := make([]string, 0, len(methods))
		for _, k := range methods {
			parts = append(parts, fmt.Sprintf("%s=%d", k, r.ops[k]))
		}
		logrus.Infof("[nodeOperator RPC] nodeId=%d %s", r.id, strings.Join(parts, " "))
	}
}
