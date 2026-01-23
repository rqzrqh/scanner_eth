package types

type RemoteChainUpdate struct {
	NodeId    int
	Height    uint64
	BlockHash string
	Weight    uint64
}
