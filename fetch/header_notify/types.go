package headernotify

// RemoteHeader is the subset we take from an eth subscription / HTTP poll.
// It never includes the transactions list; only eth_getBlockByNumber/ByHash
// returns a full header with transaction hashes for body fetch.
type RemoteHeader struct {
	Hash       string
	ParentHash string
	Number     string
	Difficulty string
}

type RemoteChainUpdate struct {
	NodeId    int
	Height    uint64
	BlockHash string
	Weight    uint64
	Header    *RemoteHeader
}
