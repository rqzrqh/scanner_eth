package fetch

import (
	"fmt"
	"os"
	"sort"

	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

type BlockDigest struct {
	Height     uint64
	Hash       string
	ParentHash string
}

// store recent block
type LocalChain struct {
	reversibleSize    int
	reversibleSection map[uint64]*BlockDigest
	startHeight       uint64
	endHeight         uint64
}

func NewLocalChain(reversibleSize int, blkDigestList []*BlockDigest) *LocalChain {
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

	return &LocalChain{
		reversibleSize:    reversibleSize,
		reversibleSection: reversibleSection,
		startHeight:       blkDigestList[0].Height,
		endHeight:         blkDigestList[len(blkDigestList)-1].Height,
	}
}

func (lc *LocalChain) Grow(height uint64, hash string, parentHash string) error {

	endBlk := lc.reversibleSection[lc.endHeight]
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

	lc.reversibleSection[height] = blk
	lc.endHeight = height

	for {
		if len(lc.reversibleSection) > lc.reversibleSize {
			delete(lc.reversibleSection, lc.startHeight)
			lc.startHeight++
		} else {
			break
		}
	}

	return nil
}

func (lc *LocalChain) Revert(height uint64) {

	if height != lc.endHeight {
		fmt.Println("memory revert failed")
		os.Exit(0)
	}

	delete(lc.reversibleSection, height)
	lc.endHeight--

	if len(lc.reversibleSection) == 0 {
		fmt.Println("memory revert out of limit")
		os.Exit(0)
	}
}

func (lc *LocalChain) GetChainInfo() (uint64, string) {

	blk := lc.reversibleSection[lc.endHeight]
	return lc.endHeight, blk.Hash
}
