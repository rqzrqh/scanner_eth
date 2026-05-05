package node

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
	nodeOperatorStats       map[int]map[string]int64
	nodeOperatorMethodStats map[int]map[string]*nodeOperatorMethodCounters
	nodeOperatorStatsOnce   sync.Once
	nodeOperatorStatsPeriod = defaultNodeOperatorStatsInterval
)

type nodeOperatorMethodCounters struct {
	Calls           int64
	TotalUS         int64
	MaxUS           int64
	ArraySamples    int64
	ArrayItemsTotal int64
	MaxArrayItems   int64
}

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

func recordNodeOperatorMethod(nodeID int, method string, elapsed time.Duration, arrayItems int) {
	if method == "" {
		return
	}
	nodeOperatorStatsOnce.Do(func() {
		go nodeOperatorStatsReporterLoop(nodeOperatorStatsPeriod)
	})
	elapsedUS := elapsed.Microseconds()
	if elapsedUS < 0 {
		elapsedUS = 0
	}
	nodeOperatorStatsMu.Lock()
	if nodeOperatorMethodStats == nil {
		nodeOperatorMethodStats = make(map[int]map[string]*nodeOperatorMethodCounters)
	}
	byMethod := nodeOperatorMethodStats[nodeID]
	if byMethod == nil {
		byMethod = make(map[string]*nodeOperatorMethodCounters)
		nodeOperatorMethodStats[nodeID] = byMethod
	}
	c := byMethod[method]
	if c == nil {
		c = &nodeOperatorMethodCounters{}
		byMethod[method] = c
	}
	c.Calls++
	c.TotalUS += elapsedUS
	if elapsedUS > c.MaxUS {
		c.MaxUS = elapsedUS
	}
	if arrayItems >= 0 {
		items := int64(arrayItems)
		c.ArraySamples++
		c.ArrayItemsTotal += items
		if items > c.MaxArrayItems {
			c.MaxArrayItems = items
		}
	}
	nodeOperatorStatsMu.Unlock()
}

func recordRPCBatchElems(n *NodeOperatorImpl, business string, elems []rpc.BatchElem) {
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

func (n *NodeOperatorImpl) recordRPC(business, rpcMethod string, delta int64) {
	if n == nil {
		return
	}
	recordNodeOperatorRPC(n.id, rpcStatKey(business, rpcMethod), delta)
}

func (n *NodeOperatorImpl) recordMethodCall(method string, startedAt time.Time, arrayItems int) {
	if n == nil {
		return
	}
	recordNodeOperatorMethod(n.id, method, time.Since(startedAt), arrayItems)
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
	if len(nodeOperatorStats) == 0 && len(nodeOperatorMethodStats) == 0 {
		nodeOperatorStatsMu.Unlock()
		return
	}
	nodeIDs := make([]int, 0, len(nodeOperatorStats)+len(nodeOperatorMethodStats))
	seenNodeIDs := make(map[int]struct{}, len(nodeOperatorStats)+len(nodeOperatorMethodStats))
	for id := range nodeOperatorStats {
		seenNodeIDs[id] = struct{}{}
		nodeIDs = append(nodeIDs, id)
	}
	for id := range nodeOperatorMethodStats {
		if _, ok := seenNodeIDs[id]; ok {
			continue
		}
		nodeIDs = append(nodeIDs, id)
	}
	sort.Ints(nodeIDs)

	type rpcRow struct {
		id  int
		ops map[string]int64
	}
	type methodRow struct {
		id      int
		methods map[string]nodeOperatorMethodCounters
	}
	rpcRows := make([]rpcRow, 0, len(nodeIDs))
	methodRows := make([]methodRow, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		src := nodeOperatorStats[id]
		if len(src) > 0 {
			cp := make(map[string]int64, len(src))
			for k, v := range src {
				cp[k] = v
			}
			rpcRows = append(rpcRows, rpcRow{id: id, ops: cp})
		}
		methodSrc := nodeOperatorMethodStats[id]
		if len(methodSrc) > 0 {
			cp := make(map[string]nodeOperatorMethodCounters, len(methodSrc))
			for k, v := range methodSrc {
				if v != nil {
					cp[k] = *v
				}
			}
			methodRows = append(methodRows, methodRow{id: id, methods: cp})
		}
	}
	nodeOperatorStatsMu.Unlock()

	for _, r := range rpcRows {
		methods := make([]string, 0, len(r.ops))
		for k := range r.ops {
			methods = append(methods, k)
		}
		sort.Strings(methods)
		parts := make([]string, 0, len(methods))
		for _, k := range methods {
			parts = append(parts, fmt.Sprintf("%s=%d", k, r.ops[k]))
		}
		logrus.Debugf("[nodeOperator RPC] nodeId=%d %s", r.id, strings.Join(parts, " "))
	}
	for _, r := range methodRows {
		methods := make([]string, 0, len(r.methods))
		for k := range r.methods {
			methods = append(methods, k)
		}
		sort.Strings(methods)
		for _, method := range methods {
			c := r.methods[method]
			avgUS := int64(0)
			if c.Calls > 0 {
				avgUS = c.TotalUS / c.Calls
			}
			avgArrayItems := int64(0)
			if c.ArraySamples > 0 {
				avgArrayItems = c.ArrayItemsTotal / c.ArraySamples
			}
			logrus.Debugf("[nodeOperator Method] nodeId=%d method=%s calls=%d total_us=%d avg_us=%d max_us=%d array_samples=%d array_items_total=%d avg_array_items=%d max_array_items=%d",
				r.id, method, c.Calls, c.TotalUS, avgUS, c.MaxUS, c.ArraySamples, c.ArrayItemsTotal, avgArrayItems, c.MaxArrayItems)
		}
	}
}
