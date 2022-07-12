package fetch

import (
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

type BlockDigest struct {
	Height     uint64
	Hash       string
	ParentHash string
}

// store recent block
type MemoryChain struct {
	mtx               sync.Mutex
	reversibleSize    int
	reversibleSection map[uint64]*BlockDigest
	startHeight       uint64
	endHeight         uint64
}

func NewMemoryChain(reversibleSize int, blkDigestList []*BlockDigest) *MemoryChain {
	if len(blkDigestList) == 0 {
		logrus.Errorf("memory incoming empty block digest list")
		os.Exit(0)
	}

	// sort
	sort.Slice(blkDigestList, func(i, j int) bool {
		return blkDigestList[i].Height < blkDigestList[j].Height
	})

	reversibleSection := make(map[uint64]*BlockDigest)
	for _, v := range blkDigestList {
		reversibleSection[v.Height] = v
	}

	return &MemoryChain{
		mtx:               sync.Mutex{},
		reversibleSize:    reversibleSize,
		reversibleSection: reversibleSection,
		startHeight:       blkDigestList[0].Height,
		endHeight:         blkDigestList[len(blkDigestList)-1].Height,
	}
}

func (mc *MemoryChain) Grow(height uint64, hash string, parentHash string) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	endBlk := mc.reversibleSection[mc.endHeight]
	if len(parentHash) != len(endBlk.Hash) {
		return xerrors.New("grow failed. hash len not equal")
	}

	for i := 0; i < len(parentHash); i++ {
		if parentHash[i] != endBlk.Hash[i] {
			return xerrors.New("grow failed. hash not equal")
		}
	}

	blk := &BlockDigest{
		Height:     height,
		Hash:       hash,
		ParentHash: parentHash,
	}

	mc.reversibleSection[height] = blk
	mc.endHeight = height

	for {
		if len(mc.reversibleSection) > mc.reversibleSize {
			delete(mc.reversibleSection, mc.startHeight)
			mc.startHeight++
		} else {
			break
		}
	}

	return nil
}

func (mc *MemoryChain) Revert(height uint64) {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	if height != mc.endHeight {
		fmt.Println("memory revert failed")
		os.Exit(0)
	}

	delete(mc.reversibleSection, height)
	mc.endHeight--

	if len(mc.reversibleSection) == 0 {
		fmt.Println("memory revert out of limit")
		os.Exit(0)
	}
}

func (mc *MemoryChain) GetChainInfo() (uint64, string) {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	blk := mc.reversibleSection[mc.endHeight]
	return mc.endHeight, blk.Hash
}
