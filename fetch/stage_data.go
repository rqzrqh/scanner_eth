package fetch

import (
	"fmt"
	"sync_eth/types"

	"golang.org/x/xerrors"
)

type DataManager struct {
	dataSet map[uint64]*types.FullBlock
}

func NewDataManager() *DataManager {
	return &DataManager{
		dataSet: make(map[uint64]*types.FullBlock),
	}
}

func (dm *DataManager) addBlock(fullblock *types.FullBlock) error {
	height := fullblock.Block.Height
	if _, exist := dm.dataSet[height]; exist {
		errlog := fmt.Sprintf("datamanager add block is exist")
		return xerrors.New(errlog)
	}

	dm.dataSet[height] = fullblock

	return nil
}

func (dm *DataManager) getBlock(height uint64) (*types.FullBlock, error) {
	if blk, exist := dm.dataSet[height]; exist {
		return blk, nil
	}

	return nil, xerrors.New("block not found")
}

func (dm *DataManager) clear() {
	dm.dataSet = nil
	dm.dataSet = make(map[uint64]*types.FullBlock)
}

func (dm *DataManager) removeData(height uint64) {
	delete(dm.dataSet, height)
}
