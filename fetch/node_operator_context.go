package fetch

import (
	"context"
	"time"
)

// defaultFetchRPCTimeout is the per-RPC context deadline when fetch.timeout is unset or zero (matches config default).
const defaultFetchRPCTimeout = 10 * time.Second

func (n *NodeOperator) withNodeRPCTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	d := defaultFetchRPCTimeout
	if n != nil && n.rpcTimeout > 0 {
		d = n.rpcTimeout
	}
	return context.WithTimeout(parent, d)
}
