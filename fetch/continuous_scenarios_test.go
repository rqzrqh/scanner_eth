package fetch

import (
	"context"
	"fmt"
	"scanner_eth/data"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

func TestContinuousGeneratedScenarios(t *testing.T) {
	fm := newTestFetchManager(t, 6)
	fm.startHeight = 1
	setNodeLatestHeight(fm, 1)
	t.Cleanup(func() { fm.taskPool.stop() })

	var mu sync.RWMutex
	headersByHeight := make(map[uint64]*BlockHeaderJson)
	headersByHash := make(map[string]*BlockHeaderJson)
	fullByHash := make(map[string]*data.FullBlock)
	bodyFailRemain := make(map[string]int)
	storedHashes := make(map[string]struct{})

	addBlock := func(height uint64, hash string, parent string, withBody bool) {
		h := makeHeader(height, hash, parent)
		mu.Lock()
		headersByHeight[height] = h
		headersByHash[hash] = h
		if withBody {
			fullByHash[hash] = makeMinimalFullBlock(height, hash, parent)
		}
		mu.Unlock()
	}

	fm.blockFetcher = &mockBlockFetcher{
		fetchByHeightFn: func(nodeID int, taskID int, client *ethclient.Client, height uint64) *BlockHeaderJson {
			mu.RLock()
			defer mu.RUnlock()
			if h, ok := headersByHeight[height]; ok {
				return h
			}
			return nil
		},
		fetchByHashFn: func(nodeID int, taskID int, client *ethclient.Client, hash string) *BlockHeaderJson {
			mu.RLock()
			defer mu.RUnlock()
			if h, ok := headersByHash[hash]; ok {
				return h
			}
			return nil
		},
		fetchFullFn: func(nodeID int, taskID int, client *ethclient.Client, header *BlockHeaderJson) *data.FullBlock {
			if header == nil {
				return nil
			}
			hash := normalizeHash(header.Hash)
			mu.Lock()
			defer mu.Unlock()
			if remain, ok := bodyFailRemain[hash]; ok && remain > 0 {
				bodyFailRemain[hash] = remain - 1
				return nil
			}
			return fullByHash[hash]
		},
	}

	fm.dbOperator = &mockDbOperator{
		storeFn: func(blockData *EventBlockData) error {
			if blockData == nil || blockData.ProtocolFullBlock == nil || blockData.ProtocolFullBlock.Block == nil {
				return nil
			}
			hash := normalizeHash(blockData.ProtocolFullBlock.Block.Hash)
			mu.Lock()
			storedHashes[hash] = struct{}{}
			mu.Unlock()
			return nil
		},
	}

	// Phase 1: linear chain growth.
	prev := ""
	for h := uint64(1); h <= 15; h++ {
		hash := fmt.Sprintf("0x%02x", h)
		addBlock(h, hash, prev, true)
		prev = hash
		setNodeLatestHeight(fm, h)
		runScanAndWait(t, fm)
	}

	// Phase 2: orphan child first, then missing parent arrives.
	addBlock(17, "0x11", "0x10", true)
	setNodeLatestHeight(fm, 17)
	fm.scanEnabled.Store(true)
	fm.scanEvents(context.Background())
	time.Sleep(50 * time.Millisecond)
	if fm.blockTree.Get("0x10") != nil {
		t.Fatalf("expected missing parent to be absent before parent header arrives")
	}

	addBlock(16, "0x10", prev, true)
	setNodeLatestHeight(fm, 17)
	runScanAndWait(t, fm)
	if fm.blockTree.Get("0x10") == nil || fm.blockTree.Get("0x11") == nil {
		t.Fatalf("expected orphan parent/child to be linked after parent arrives")
	}
	prev = "0x11"

	// Phase 3: intermittent body failures, then retry success.
	for h := uint64(18); h <= 25; h++ {
		hash := fmt.Sprintf("0x%02x", h)
		addBlock(h, hash, prev, true)
		if h%2 == 0 {
			mu.Lock()
			bodyFailRemain[hash] = 2
			mu.Unlock()
		}
		prev = hash
		setNodeLatestHeight(fm, h)
		runScanAndWait(t, fm)
	}

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		runScanAndWait(t, fm)
		_, end, ok := fm.blockTree.HeightRange()
		mu.RLock()
		storedCount := len(storedHashes)
		mu.RUnlock()
		if ok && end >= 24 && storedCount >= 18 {
			return
		}
	}

	_, end, ok := fm.blockTree.HeightRange()
	mu.RLock()
	storedCount := len(storedHashes)
	mu.RUnlock()
	t.Fatalf("expected blocktree to reach latest generated height and enough stored blocks, got ok=%v end=%v stored=%v", ok, end, storedCount)
}
