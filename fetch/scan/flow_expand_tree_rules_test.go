package scan

import (
	"context"
	fetcherpkg "scanner_eth/fetch/fetcher"
	fetchstore "scanner_eth/fetch/store"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

func TestRunScanCycleRule1BootstrapOnEmptyTree(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.setStartHeight(100)
	var calls atomic.Int32
	env.setLatestRemote(100)
	env.fetchHeaderByHeightFn = func(_ context.Context, height uint64) *fetcherpkg.BlockHeaderJson {
		if height != 100 {
			t.Fatalf("unexpected bootstrap height: %v", height)
		}
		calls.Add(1)
		return makeTestHeader(height, "0xaaa", "")
	}

	env.runScanAndWait()
	if calls.Load() != 1 {
		t.Fatalf("bootstrap header fetch count mismatch: got=%v want=1", calls.Load())
	}

	start, end, ok := env.blockTree.HeightRange()
	if !ok || start != 100 || end != 100 {
		t.Fatalf("bootstrap range mismatch: ok=%v start=%v end=%v", ok, start, end)
	}
}

func TestRunScanCycleRule2WindowExpandsToDoubleIrreversible(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0xa", ""))
	env.setLatestRemote(13)
	var calls atomic.Int32
	env.fetchHeaderByHeightFn = func(_ context.Context, height uint64) *fetcherpkg.BlockHeaderJson {
		calls.Add(1)
		switch height {
		case 11:
			return makeTestHeader(11, "0xb", "0xa")
		case 12:
			return makeTestHeader(12, "0xc", "0xb")
		case 13:
			return makeTestHeader(13, "0xd", "0xc")
		default:
			return nil
		}
	}
	env.fetchBodyByHashFn = func(_ context.Context, hash string, _ uint64, header *fetcherpkg.BlockHeaderJson) (*fetchstore.EventBlockData, int, int64, bool) {
		if header == nil {
			return nil, -1, 0, false
		}
		h := uint64(0)
		if header.Number != "" {
			if v, err := hexutil.DecodeUint64(header.Number); err == nil {
				h = v
			}
		}
		return makeTestEventBlockData(h, normalizeTestHash(header.Hash), normalizeTestHash(header.ParentHash)), -1, 0, true
	}

	env.runScanAndWait()
	start, end, ok := env.blockTree.HeightRange()
	if !ok || start != 10 || end < 11 {
		t.Fatalf("range mismatch after window sync: ok=%v start=%v end=%v", ok, start, end)
	}
	if calls.Load() < 1 {
		t.Fatalf("window sync call mismatch: got=%v want>=1", calls.Load())
	}
}
