package headernotify

// RemoteHeader is the subset we take from an eth subscription / HTTP poll.
// These fields are enough for scan to validate a continuous header-by-hash
// candidate, but not enough to insert/cache block data because the transaction
// list and full block fields are absent.
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
