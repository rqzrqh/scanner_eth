package scan

import (
	"context"
	fetcherpkg "scanner_eth/fetch/fetcher"
	"testing"
)

func TestPlanP3HeaderFetchTimeoutAndDisorderConverges(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0x0a", ""))
	env.setLatestRemote(12)

	height11Calls := 0
	env.fetchHeaderByHashFn = func(_ context.Context, hash string) *fetcherpkg.BlockHeaderJson {
		if hash != "0x0c" {
			return nil
		}
		return makeTestHeader(12, "0x0c", "0x0b")
	}
	env.fetchHeaderByHeightFn = func(_ context.Context, height uint64) *fetcherpkg.BlockHeaderJson {
		if height != 11 {
			return nil
		}
		height11Calls++
		if height11Calls == 1 {
			return nil
		}
		return makeTestHeader(11, "0x0b", "0x0a")
	}

	if ok := env.flow.taskRuntime.FetchAndInsertHeaderByHash("0x0c"); !ok {
		t.Fatal("expected child header insertion by hash to succeed")
	}
	if env.blockTree.Get("0x0c") != nil {
		t.Fatal("child should stay orphan before parent arrives")
	}

	if got := env.flow.taskRuntime.FetchAndInsertHeaderByHeight(11); got != nil {
		t.Fatal("expected first height-11 fetch to fail (timeout simulation)")
	}
	if got := env.flow.taskRuntime.FetchAndInsertHeaderByHeight(11); got == nil {
		t.Fatal("expected second height-11 fetch to recover and succeed")
	}

	n11 := env.blockTree.Get("0x0b")
	if n11 == nil {
		t.Fatal("expected height-11 header to converge into blocktree")
	}
	n12 := env.blockTree.Get("0x0c")
	if n12 == nil {
		t.Fatal("expected height-12 header to converge into blocktree")
	}
	if n12.ParentKey != "0x0b" {
		t.Fatalf("unexpected parent for 0x0c: got=%s want=0x0b", n12.ParentKey)
	}
	if n11.Height != 11 || n12.Height != 12 {
		t.Fatalf("unexpected linked heights: h11=%d h12=%d", n11.Height, n12.Height)
	}
}
