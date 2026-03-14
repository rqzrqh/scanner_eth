# Test report (2026-04-13)

> **Historical snapshot.** Figures below reflect `go test`/coverage from this date. The module layout may have changed since—for example, **`scanner_eth/publish` is not present in the current tree** (see §4.3–4.4), but numbers are kept for the record. Regenerate coverage locally if you need current percentages.

**Related:** [README.md](../README.md) · [FormalVerification.md](../FormalVerification.md) · [BlockTree.md](../BlockTree.md) · [design.md](design.md) · [README.md in this folder](README.md) (`doc/` index) · [Latest coverage (2026-04-23)](test_report_2026-04-23.md)

## 1. Commands run

```bash
go test ./... -count=1 -coverprofile=coverage.out -covermode=atomic
go tool cover -func=coverage.out
```

```bash
go test ./util -count=1 -coverprofile=coverage_util.out
go tool cover -func=coverage_util.out
```

## 2. Code changes

- **File:** `util/util.go`
- **Area:** `HandleErrorWithRetry`
  - Cache the function name to avoid `runtime`/`reflect` lookup on every retry.
  - Return immediately when the retry budget is exhausted, avoiding a pointless final `sleep`.
  - Handle a nil handler by returning nil, avoiding a potential panic.

## 3. New unit tests

- **File:** `util/util_test.go`
- **Functions covered:**
  - `ToBlockNumArg`
  - `HandleErrorWithRetry`
  - `HitNoMoreRetryErrors`
- **Scenarios:**
  - `latest` / `pending` / ordinary height encoding
  - success after retries
  - error when max retries exceeded
  - nil handler returns immediately
  - non-retryable error string matching

## 4. Coverage results

### 4.1 `util` package

- `scanner_eth/util`: **100.0%**
- **By function:**
  - `ToBlockNumArg`: 100.0%
  - `HandleErrorWithRetry`: 100.0%
  - `HitNoMoreRetryErrors`: 100.0%

### 4.2 `blocktree` package (new)

- `scanner_eth/blocktree`: **100.0%**
- **Notable functions:**
  - `computeIrreversible`: 100.0%
  - `Prune`: 100.0%
  - `Branches`: 100.0%
  - `walk` / `SetHeader` / `Root` / `Insert` / `internalInsert`: 100.0%

### 4.3 `publish` package

- `scanner_eth/publish`: **71.2%**
- **By function:**
  - `publishEvent`: 81.2%
  - `onBecameLeader` / `onLostLeader` / `stopPublishLoop`: 100.0%
  - `startPublishLoop`: 58.8%

### 4.4 Repository total

- **Overall:** 35.5%
- **Selected packages:**
  - `scanner_eth/blocktree`: 100.0%
  - `scanner_eth/fetch`: 54.9%
  - `scanner_eth/leader`: 61.8%
  - `scanner_eth/publish`: 71.2%
  - `scanner_eth/util`: 100.0%

### 4.5 `fetch` package (further work)

- `scanner_eth/fetch`: **54.9%**
- **Notable improvements this round:**
  - `cache_erc20.Get` / `Set`: 100.0%
  - `cache_erc721.Get` / `Set`: 100.0%
  - `convert.SetOptionalFeatures`: 100.0%
  - `convert.ConvertStorageFullBlock`: 98.6%
  - `convert.ConvertProtocolFullBlock`: 45.1%
  - `header_notifier.NewHeaderNotifier` / `toRemoteHeader`: 100.0%
  - `header_notifier.Run`: 25.0%
  - `fetcher.SetEnableInternalTx` / `NewBlockFetcher` / `transTraceAddressToString`: 100.0%
  - `fetcher.normalizeTraceAddress` / `parseTraceBigInt` / `parseTxInternal`: 100.0%
  - `db_operator.NewDbOperator`: 100.0%
  - `db_operator.StoreBlockData`: 66.7%
  - `fetch_manager.Run` / `Stop` / `startHeaderNotifiersAndConsumer`: materially improved
  - `fetch_manager.onLostLeader` / `triggerScan` / `isScanEnabled` / `stopScanLoop`: 100.0%
  - `node_manager.GetChainInfo` / `NodeCount`: 100.0%
  - `scan_flow.scanStageName`: 100.0%
  - `prune_state.capturePruneStateSnapshot` / `logPruneSnapshot` / `storedHeightRangeOnTree`: 100.0%
  - `prune_state.pruneStoredBlocks`: 95.8%
  - `restore_tree.restoreBlockTree`: 95.9%
  - `restore_tree.parseStoredBlockWeight`: 100.0%
  - `sync_block_data.syncNodeDataByHash` / `insertHeader` / `headerWeight`: 100.0%
  - `scan_flow.scanEvents` / `syncHeaderByHeightTarget` / `processBranchNode` / `shouldSyncOrphanParent`: edge paths filled in

## 5. Coverage delta

- **Before this round:** 33.0% overall
- **After this round:** 35.5% overall
- **Change:** +2.5 pp

## 6. Conclusion

- Optimizations are merged and the full test suite passes.
- `util` reached 100% coverage and helped lift overall coverage.
- Remaining weak spots: packages with little or no tests yet, e.g. `main`, `middleware`, `publish`, `syncer`, `model` (as of this snapshot).
