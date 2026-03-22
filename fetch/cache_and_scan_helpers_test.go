package fetch

import (
	"fmt"
	"testing"
	"time"

	"scanner_eth/data"
	"scanner_eth/model"

	"github.com/ethereum/go-ethereum/ethclient"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newErc20CacheTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite db failed: %v", err)
	}
	if err := db.AutoMigrate(&model.ContractErc20{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	return db
}

func newErc721CacheTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite db failed: %v", err)
	}
	if err := db.AutoMigrate(&model.ContractErc721{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	return db
}

func TestTokenCacheGetSetAndDBFallback(t *testing.T) {
	cache := newTokenCache()

	if got, ok := cache.Get("0xnotfound", nil); ok || got != nil {
		t.Fatalf("expected miss for empty cache")
	}

	v := makeContractErc20("0x1")
	cache.Set("0x1", v)
	if got, ok := cache.Get("0x1", nil); !ok || got == nil || got.ContractAddr != "0x1" {
		t.Fatalf("expected memory cache hit, got=%v ok=%v", got, ok)
	}

	cache.Set("0xnil", nil)
	if _, exists := cache.items["0xnil"]; exists {
		t.Fatal("nil set should not create cache item")
	}

	cache.items["0x1"].expireAt = time.Now().Add(-time.Second)
	if got, ok := cache.Get("0x1", nil); ok || got != nil {
		t.Fatalf("expected expired item miss, got=%v ok=%v", got, ok)
	}
	if _, exists := cache.items["0x1"]; exists {
		t.Fatal("expired item should be removed from cache")
	}

	db := newErc20CacheTestDB(t)
	if err := db.Create(&model.ContractErc20{ContractAddr: "0xdb20", Name: "Token20", Symbol: "T20", Decimals: 18, TotalSupply: "1000"}).Error; err != nil {
		t.Fatalf("insert contract_erc20 failed: %v", err)
	}
	fromDB, ok := cache.Get("0xdb20", db)
	if !ok || fromDB == nil || fromDB.Symbol != "T20" {
		t.Fatalf("expected db fallback hit, got=%v ok=%v", fromDB, ok)
	}
	if _, ok := cache.Get("0xdb20", nil); !ok {
		t.Fatal("db fallback result should be refilled into memory cache")
	}
}

func TestErc721CacheGetSetAndDBFallback(t *testing.T) {
	cache := newErc721ContractCache()

	if got, ok := cache.Get("0xnotfound", nil); ok || got != nil {
		t.Fatalf("expected miss for empty cache")
	}

	v := makeContractErc721("0x2")
	cache.Set("0x2", v)
	if got, ok := cache.Get("0x2", nil); !ok || got == nil || got.ContractAddr != "0x2" {
		t.Fatalf("expected memory cache hit, got=%v ok=%v", got, ok)
	}

	cache.Set("0xnil", nil)
	if _, exists := cache.items["0xnil"]; exists {
		t.Fatal("nil set should not create cache item")
	}

	cache.items["0x2"].expireAt = time.Now().Add(-time.Second)
	if got, ok := cache.Get("0x2", nil); ok || got != nil {
		t.Fatalf("expected expired item miss, got=%v ok=%v", got, ok)
	}
	if _, exists := cache.items["0x2"]; exists {
		t.Fatal("expired item should be removed from cache")
	}

	db := newErc721CacheTestDB(t)
	if err := db.Create(&model.ContractErc721{ContractAddr: "0xdb721", Name: "NFT", Symbol: "N"}).Error; err != nil {
		t.Fatalf("insert contract_erc721 failed: %v", err)
	}
	fromDB, ok := cache.Get("0xdb721", db)
	if !ok || fromDB == nil || fromDB.Name != "NFT" {
		t.Fatalf("expected db fallback hit, got=%v ok=%v", fromDB, ok)
	}
	if _, ok := cache.Get("0xdb721", nil); !ok {
		t.Fatal("db fallback result should be refilled into memory cache")
	}
}

func TestScanStageName(t *testing.T) {
	if got := scanStageName(scanStageHeaderByHeightDone); got != "header_by_height" {
		t.Fatalf("unexpected stage name: %s", got)
	}
	if got := scanStageName(scanStageHeaderByHashDone); got != "header_by_hash" {
		t.Fatalf("unexpected stage name: %s", got)
	}
	if got := scanStageName(scanStageBodyDone); got != "body" {
		t.Fatalf("unexpected stage name: %s", got)
	}
	if got := scanStageName(scanStage(99)); got != "unknown" {
		t.Fatalf("unexpected stage name for unknown stage: %s", got)
	}
}

func TestSyncHeaderWindowAndSyncOrphanParents(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.startHeight = 5
	setNodeLatestHeight(fm, 7)
	t.Cleanup(func() { fm.taskPool.stop() })

	fm.blockFetcher = &mockBlockFetcher{
		fetchByHeightFn: func(nodeID int, taskID int, client *ethclient.Client, height uint64) *BlockHeaderJson {
			switch height {
			case 5:
				return makeHeader(5, "0x05", "")
			case 6:
				return makeHeader(6, "0x06", "0x05")
			case 7:
				return makeHeader(7, "0x07", "0x06")
			default:
				return nil
			}
		},
		fetchByHashFn: func(nodeID int, taskID int, client *ethclient.Client, hash string) *BlockHeaderJson {
			if normalizeHash(hash) == "0x10" {
				return makeHeader(16, "0x10", "0x07")
			}
			return nil
		},
	}

	fm.syncHeaderWindow()
	start, end, ok := fm.blockTree.HeightRange()
	if !ok || start != 5 || end != 7 {
		t.Fatalf("unexpected height range after syncHeaderWindow: ok=%v start=%v end=%v", ok, start, end)
	}

	// orphan child waits for parent 0x10.
	fm.blockTree.Insert(17, "0x11", "0x10", 1, nil, nil)
	fm.syncOrphanParents()
	if fm.blockTree.Get("0x10") == nil || fm.blockTree.Get("0x11") == nil {
		t.Fatal("expected orphan parent and child to be linked after syncOrphanParents")
	}
}

func TestGetHeaderByHeightSyncTargetsFormalPredicates(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.startHeight = 5
	t.Cleanup(func() { fm.taskPool.stop() })

	// Empty tree should bootstrap from startHeight when that height is not already syncing.
	targets := fm.getHeaderByHeightSyncTargets()
	if len(targets) != 1 || targets[0] != 5 {
		t.Fatalf("unexpected bootstrap targets: %v", targets)
	}

	if !fm.taskPool.tryStartHeaderHeightSync(5) {
		t.Fatal("failed to seed header-height syncing state")
	}
	if targets := fm.getHeaderByHeightSyncTargets(); len(targets) != 0 {
		t.Fatalf("expected no bootstrap targets while startHeight is syncing, got=%v", targets)
	}
	fm.taskPool.finishHeaderHeightSync(5)

	// Non-empty tree should produce a continuous window-bounded height range.
	fm.insertHeader(makeHeader(5, "0x05", ""))
	fm.insertHeader(makeHeader(6, "0x06", "0x05"))
	setNodeLatestHeight(fm, 10)

	targets = fm.getHeaderByHeightSyncTargets()
	want := []uint64{7, 8}
	if len(targets) != len(want) {
		t.Fatalf("unexpected target count: got=%v want=%v", targets, want)
	}
	for i, h := range want {
		if targets[i] != h {
			t.Fatalf("unexpected target at index %d: got=%d want=%d all=%v", i, targets[i], h, targets)
		}
	}

	if !fm.taskPool.tryStartHeaderHeightSync(7) {
		t.Fatal("failed to seed syncing state for height 7")
	}
	targets = fm.getHeaderByHeightSyncTargets()
	if len(targets) != 1 || targets[0] != 8 {
		t.Fatalf("expected only non-syncing height to remain, got=%v", targets)
	}
	fm.taskPool.finishHeaderHeightSync(7)

	// Latest remote height is also an upper bound.
	setNodeLatestHeight(fm, 7)
	targets = fm.getHeaderByHeightSyncTargets()
	if len(targets) != 1 || targets[0] != 7 {
		t.Fatalf("expected latest height bound to limit targets to [7], got=%v", targets)
	}
}

func TestGetHeaderByHashSyncTargetsFormalPredicates(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	fm.insertHeader(makeHeader(10, "0x0a", ""))
	fm.blockTree.Insert(11, "0x0b", "0x0f", 1, nil, nil)
	fm.blockTree.Insert(12, "0x0c", "0x10", 1, nil, nil)

	targets := fm.getHeaderByHashSyncTargets()
	if len(targets) != 2 {
		t.Fatalf("expected two missing parent targets, got=%v", targets)
	}
	targetSet := make(map[string]struct{}, len(targets))
	for _, hash := range targets {
		targetSet[hash] = struct{}{}
		if hash == "" {
			t.Fatal("empty hash must not appear in hash sync targets")
		}
		if fm.blockTree.Get(hash) != nil {
			t.Fatalf("existing node hash must not appear as sync target: %s", hash)
		}
	}
	if _, ok := targetSet["0x0f"]; !ok {
		t.Fatalf("expected normalized missing parent 0x0f in targets: %v", targets)
	}
	if _, ok := targetSet["0x10"]; !ok {
		t.Fatalf("expected normalized missing parent 0x10 in targets: %v", targets)
	}

	if !fm.taskPool.tryStartHeaderHashSync("0x0f") {
		t.Fatal("failed to seed header-hash syncing state")
	}
	targets = fm.getHeaderByHashSyncTargets()
	if len(targets) != 1 || targets[0] != "0x10" {
		t.Fatalf("expected syncing hash to be filtered out, got=%v", targets)
	}
	fm.taskPool.finishHeaderHashSync("0x0f")
}

func TestHeightSyncAdvancesExactlyByDerivedTargets(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.startHeight = 5
	t.Cleanup(func() { fm.taskPool.stop() })

	fm.insertHeader(makeHeader(5, "0x05", ""))
	fm.insertHeader(makeHeader(6, "0x06", "0x05"))
	setNodeLatestHeight(fm, 10)

	fm.blockFetcher = &mockBlockFetcher{
		fetchByHeightFn: func(nodeID int, taskID int, client *ethclient.Client, height uint64) *BlockHeaderJson {
			switch height {
			case 7:
				return makeHeader(7, "0x07", "0x06")
			case 8:
				return makeHeader(8, "0x08", "0x07")
			default:
				return nil
			}
		},
	}

	start, initialEnd, ok := fm.blockTree.HeightRange()
	if !ok || start != 5 || initialEnd != 6 {
		t.Fatalf("unexpected initial height range: ok=%v start=%v end=%v", ok, start, initialEnd)
	}

	targets := fm.getHeaderByHeightSyncTargets()
	if len(targets) != 2 {
		t.Fatalf("expected two derived height targets, got=%v", targets)
	}

	fm.syncHeaderWindow()

	_, finalEnd, ok := fm.blockTree.HeightRange()
	if !ok {
		t.Fatal("expected non-empty height range after syncHeaderWindow")
	}
	wantEnd := initialEnd + uint64(len(targets))
	if finalEnd != wantEnd {
		t.Fatalf("unexpected height advancement: got end=%d want=%d targets=%v", finalEnd, wantEnd, targets)
	}
}

func TestHeaderHashSyncFailureLeavesTargetRetryable(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	fm.insertHeader(makeHeader(10, "0x0a", ""))
	fm.blockTree.Insert(11, "0x0b", "0x0f", 1, nil, nil)
	fm.blockFetcher = &mockBlockFetcher{
		fetchByHashFn: func(nodeID int, taskID int, client *ethclient.Client, hash string) *BlockHeaderJson {
			return nil
		},
	}

	if fm.fetchAndInsertHeaderByHash("0x0f") {
		t.Fatal("expected hash sync failure when fetcher returns nil")
	}
	if fm.blockTree.Get("0x0f") != nil {
		t.Fatal("failed hash sync must not insert parent into tree")
	}
	if fm.storedBlocks.IsStored("0x0f") {
		t.Fatal("failed hash sync must not mark parent as stored")
	}

	targets := fm.getHeaderByHashSyncTargets()
	if len(targets) != 1 || targets[0] != "0x0f" {
		t.Fatalf("failed hash sync should remain retryable in next target set, got=%v", targets)
	}
}

func TestCountActionableAndStoredLinkedNodes(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	fm.blockTree.Insert(10, "0xaa", "", 1, nil, nil)
	fm.blockTree.Insert(11, "0xbb", "0xaa", 1, nil, nil)

	// Root has storable data and parent is ready; child has nil data.
	fm.setNodeBlockBody("0xaa", makeEventBlockData(10, "0xaa", ""))

	if got := fm.countActionableBodyNodes(); got != 2 {
		t.Fatalf("expected 2 actionable nodes (root storable + child nil data), got=%d", got)
	}

	fm.storedBlocks.MarkStored("0xaa")
	if got := fm.countStoredLinkedNodes(); got != 1 {
		t.Fatalf("expected 1 stored linked node, got=%d", got)
	}
}

func makeContractErc20(addr string) *data.ContractErc20 {
	return &data.ContractErc20{ContractAddr: addr, Name: "n", Symbol: "s", Decimals: 18, TotalSupply: "1"}
}

func makeContractErc721(addr string) *data.ContractErc721 {
	return &data.ContractErc721{ContractAddr: addr, Name: "nft", Symbol: "n"}
}
