package fetch

type RemoteChain struct {
	height uint64
	hash   string
}

func NewRemoteChain() *RemoteChain {
	return &RemoteChain{}
}

func (c *RemoteChain) Update(height uint64, hash string) {
	c.height = height
	c.hash = hash
}

func (c *RemoteChain) GetChainInfo() (uint64, string) {
	return c.height, c.hash
}
