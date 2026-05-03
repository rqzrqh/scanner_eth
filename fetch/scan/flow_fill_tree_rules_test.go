package scan

import (
	"context"
	fetcherpkg "scanner_eth/fetch/fetcher"
	"testing"
)

func TestRunScanCycleRule3SyncOrphanParentsByHash(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0xroot", ""))
	env.blockTree.Insert(12, "0xchild", "0xmissing", 1)
	env.setLatestRemote(10)
	env.fetchHeaderByHashFn = func(_ context.Context, hash string) *fetcherpkg.BlockHeaderJson {
		if hash != "0xmissing" {
			return nil
		}
		return makeTestHeader(11, "0xmissing", "0xroot")
	}

	env.runScanAndWait()
	if env.blockTree.Get("0xmissing") == nil {
		t.Fatalf("missing parent should be inserted")
	}
	if env.blockTree.Get("0xchild") == nil {
		t.Fatalf("orphan child should be linked after parent sync")
	}
}
