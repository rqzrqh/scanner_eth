package scan

import (
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

func (sf *Flow) EnqueueRemoteHeaderCandidate(blockHash, headerHash, parentHash, number string) bool {
	if sf == nil {
		return false
	}
	sf.BindRuntimeDeps()
	if sf.blockTree == nil || sf.taskPool == nil {
		return false
	}

	hash := sf.normalize(blockHash)
	normalizedHeaderHash := sf.normalize(headerHash)
	if hash == "" || normalizedHeaderHash == "" || hash != normalizedHeaderHash {
		return false
	}
	if sf.blockTree.Get(hash) != nil || sf.taskPool.IsHeaderHashSyncing(hash) {
		return false
	}

	parent := sf.blockTree.Get(sf.normalize(parentHash))
	if parent == nil {
		return false
	}

	height, err := hexutil.DecodeUint64(strings.TrimSpace(number))
	if err != nil || height != parent.Height+1 {
		return false
	}

	return sf.taskPool.EnqueueHeaderHashTask(hash)
}
