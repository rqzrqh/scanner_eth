package headernotify

// RemoteHeader is the subset we take from an eth subscription / HTTP poll.
// These fields are enough to extend the block tree, but not enough to seed
// pending block data because the transactions list is absent.
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
	Header    *RemoteHeader
}
