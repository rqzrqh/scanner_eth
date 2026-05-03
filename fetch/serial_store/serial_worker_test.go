package serialstore

import (
	"context"
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/model"
	"testing"
)

type serialTestBlockStorer struct {
	storeFn func(context.Context, *fetchstore.EventBlockData) error
}

func (s serialTestBlockStorer) StoreBlockData(ctx context.Context, data *fetchstore.EventBlockData) error {
	if s.storeFn == nil {
		return nil
	}
	return s.storeFn(ctx, data)
}

func makeSerialTestEventBlockData(height uint64, hash, parent string) *fetchstore.EventBlockData {
	return &fetchstore.EventBlockData{
		StorageFullBlock: &fetchstore.StorageFullBlock{
			Block: model.Block{
				Height:     height,
				Hash:       hash,
				ParentHash: parent,
				Complete:   true,
			},
		},
	}
}

func TestSubmitBranchesStoresLowToHighFullBranch(t *testing.T) {
	stored := fetchstore.NewStoredBlockState()
	stored.MarkStored("a")

	order := make([]string, 0, 1)
	worker := NewStartedWorker(serialTestBlockStorer{
		storeFn: func(_ context.Context, data *fetchstore.EventBlockData) error {
			if data != nil && data.StorageFullBlock != nil {
				order = append(order, data.StorageFullBlock.Block.Hash)
			}
			return nil
		},
	}, &stored, func(data *fetchstore.EventBlockData) bool {
		return data == nil || data.StorageFullBlock == nil
	})
	defer worker.Stop()

	branch := Branch{
		Nodes: []BranchNode{
			{
				Hash:      "b",
				ParentHash: "a",
				Height:    2,
				BlockData: makeSerialTestEventBlockData(2, "b", "a"),
			},
		},
	}

	if err := worker.SubmitBranches(context.Background(), []Branch{branch}); err != nil {
		t.Fatalf("submit branches failed: %v", err)
	}
	if len(order) != 1 || order[0] != "b" {
		t.Fatalf("expected low-to-high full branch head to store, got=%v", order)
	}
}

func TestSubmitBranchesUsesBranchLocalProgressForChildren(t *testing.T) {
	stored := fetchstore.NewStoredBlockState()
	stored.MarkStored("a")

	order := make([]string, 0, 2)
	worker := NewStartedWorker(serialTestBlockStorer{
		storeFn: func(_ context.Context, data *fetchstore.EventBlockData) error {
			if data != nil && data.StorageFullBlock != nil {
				order = append(order, data.StorageFullBlock.Block.Hash)
			}
			return nil
		},
	}, &stored, func(data *fetchstore.EventBlockData) bool {
		return data == nil || data.StorageFullBlock == nil
	})
	defer worker.Stop()

	branch := Branch{
		Nodes: []BranchNode{
			{
				Hash:      "b",
				ParentHash: "a",
				Height:    2,
				BlockData: makeSerialTestEventBlockData(2, "b", "a"),
			},
			{
				Hash:      "c",
				ParentHash: "b",
				Height:    3,
				BlockData: makeSerialTestEventBlockData(3, "c", "b"),
			},
		},
	}

	if err := worker.SubmitBranches(context.Background(), []Branch{branch}); err != nil {
		t.Fatalf("submit branches failed: %v", err)
	}
	if len(order) != 2 || order[0] != "b" || order[1] != "c" {
		t.Fatalf("expected branch-local parent progression to allow b->c, got=%v", order)
	}
}
