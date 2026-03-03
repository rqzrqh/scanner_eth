package fetch

import (
	"fmt"
	"scanner_eth/data"

	"golang.org/x/xerrors"
)

type PendingBlocks struct {
	dataSet map[uint64]*data.FullBlock
}

func NewPendingBlocks() *PendingBlocks {
	return &PendingBlocks{
		dataSet: make(map[uint64]*data.FullBlock),
	}
}

func (pb *PendingBlocks) addBlock(fullblock *data.FullBlock) error {
	height := fullblock.Block.Height
	if _, exist := pb.dataSet[height]; exist {
		errlog := fmt.Sprintf("PendingBlocks add block is exist")
		return xerrors.New(errlog)
	}

	pb.dataSet[height] = fullblock

	return nil
}

func (pb *PendingBlocks) getBlock(height uint64) (*data.FullBlock, error) {
	if blk, exist := pb.dataSet[height]; exist {
		return blk, nil
	}

	return nil, xerrors.New("block not found")
}

func (pb *PendingBlocks) clear() {
	pb.dataSet = nil
	pb.dataSet = make(map[uint64]*data.FullBlock)
}

func (pb *PendingBlocks) removeData(height uint64) {
	delete(pb.dataSet, height)
}
