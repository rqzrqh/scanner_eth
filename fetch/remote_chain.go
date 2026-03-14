package fetch

type RemoteChain struct {
	height uint64
	hash   string
}

func NewRemoteChain() *RemoteChain {
	return &RemoteChain{}
}

// Update records the remote tip reported by HeaderNotifier or RPC.
// Heights that are lower than the last observed tip are ignored so that
// transient stale notifications (replayed or out-of-order heads) cannot
// shrink GetLatestHeight and break header-window predicates (see FormalVerification.md, invariant I6).
// A canonical reorg to a strictly lower block height is not represented here;
// recovery would require an explicit resync path if that becomes a requirement.
func (c *RemoteChain) Update(height uint64, hash string) {
	if height < c.height {
		return
	}
	c.height = height
	c.hash = hash
}

func (c *RemoteChain) GetChainInfo() (uint64, string) {
	return c.height, c.hash
}
