package headernotify

type RemoteChain struct {
	height uint64
	hash   string
}

func NewRemoteChain() *RemoteChain {
	return &RemoteChain{}
}

// Update records the remote tip reported by the header notifier or RPC.
// Heights that are lower than the last observed tip are ignored so that
// transient stale notifications (replayed or out-of-order heads) cannot
// shrink GetLatestHeight and break header-window predicates.
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
