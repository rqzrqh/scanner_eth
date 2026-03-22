# 测试报告（2026-04-13）

## 1. 执行命令

```bash
go test ./... -count=1 -coverprofile=coverage.out -covermode=atomic
go tool cover -func=coverage.out
```

```bash
go test ./util -count=1 -coverprofile=coverage_util.out
go tool cover -func=coverage_util.out
```

## 2. 代码优化项

- 文件：util/util.go
- 优化点：HandleErrorWithRetry
  - 缓存函数名，避免每次重试做 runtime/reflect 查找
  - 达到重试上限后立即返回，避免最后一次失败后无意义 sleep
  - 处理 nil handler，直接返回 nil，避免潜在 panic

## 3. 新增单元测试

- 文件：util/util_test.go
- 覆盖函数：
  - ToBlockNumArg
  - HandleErrorWithRetry
  - HitNoMoreRetryErrors
- 主要场景：
  - latest/pending/普通高度编码
  - 重试后成功
  - 达到重试上限返回错误
  - nil handler 立即返回
  - 不可重试错误关键字匹配

## 4. 覆盖率结果

### 4.1 util 包

- scanner_eth/util: 100.0%
- 函数覆盖：
  - ToBlockNumArg: 100.0%
  - HandleErrorWithRetry: 100.0%
  - HitNoMoreRetryErrors: 100.0%

### 4.2 blocktree 包（新增）

- scanner_eth/blocktree: 100.0%
- 关键函数覆盖：
  - computeIrreversible: 100.0%
  - Prune: 100.0%
  - Branches: 100.0%
  - walk/SetHeader/Root/Insert/internalInsert: 100.0%

### 4.3 publish 包

- scanner_eth/publish: 71.2%
- 函数覆盖：
  - publishEvent: 81.2%
  - onBecameLeader/onLostLeader/stopPublishLoop: 100.0%
  - startPublishLoop: 58.8%

### 4.4 仓库整体

- 总覆盖率：35.5%
- 关键包覆盖率：
  - scanner_eth/blocktree: 100.0%
  - scanner_eth/fetch: 54.9%
  - scanner_eth/leader: 61.8%
  - scanner_eth/publish: 71.2%
  - scanner_eth/util: 100.0%

### 4.5 fetch 包（继续优化）

- scanner_eth/fetch: 54.9%
- 本轮主要提升点：
  - cache_erc20.Get/Set: 100.0%
  - cache_erc721.Get/Set: 100.0%
  - convert.SetOptionalFeatures: 100.0%
  - convert.ConvertStorageFullBlock: 98.6%
  - convert.ConvertProtocolFullBlock: 45.1%
  - header_notifier.NewHeaderNotifier/toRemoteHeader: 100.0%
  - header_notifier.Run: 25.0%
  - fetcher.SetEnableInternalTx/NewBlockFetcher/transTraceAddressToString: 100.0%
  - fetcher.normalizeTraceAddress/parseTraceBigInt/parseTxInternal: 100.0%
  - db_operator.NewDbOperator: 100.0%
  - db_operator.StoreBlockData: 66.7%
  - fetch_manager.Run/Stop/startHeaderNotifiersAndConsumer 路径显著提升
  - fetch_manager.onLostLeader/triggerScan/isScanEnabled/stopScanLoop: 100.0%
  - node_manager.GetChainInfo/NodeCount: 100.0%
  - scan_flow.scanStageName: 100.0%
  - prune_state.capturePruneStateSnapshot/logPruneSnapshot/storedHeightRangeOnTree: 100.0%
  - prune_state.pruneStoredBlocks: 95.8%
  - restore_tree.restoreBlockTree: 95.9%
  - restore_tree.parseStoredBlockWeight: 100.0%
  - sync_block_data.syncNodeDataByHash/insertHeader/headerWeight: 100.0%
  - scan_flow.scanEvents/syncHeaderByHeightTarget/processBranchNode/shouldSyncOrphanParent 等边界路径补齐

## 5. 覆盖率提升

- 本轮改造前整体覆盖率：33.0%
- 本轮改造后整体覆盖率：35.5%
- 提升：+2.5%

## 6. 结论

- 代码优化已落地并通过全量测试。
- util 包覆盖率提升至 100%，并带动整体覆盖率提升。
- 覆盖率主要薄弱点仍在 main/middleware/publish/syncer/model 等未建立单测的模块。
