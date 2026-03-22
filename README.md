# scanner_eth

## 形式化验证快速审计

以下命令用于快速检查 C1-C6 对应的关键回归点。

一键执行（推荐）：

```bash
go test ./fetch -run 'TestInvariant(HeaderInsertThenPending|HeaderInsertRejectedStillWritesPendingHeader|SetBlockBodyNoImplicitCreate|StoredOnlyAfterSuccessfulStore|PruneDeletesPendingAndStored)|TestRestoreBlockTreeLoadsWindowAndCompleteState|TestScanEventsRule5PruneRemovesStoredAndTasks' -count=1 && go test ./blocktree -run 'TestInvariant(PruneReturnsRemovedOrphans|OrphansAboveRootAfterPrune)' -count=1
```

可读两行版：

```bash
go test ./fetch -run 'TestInvariant(HeaderInsertThenPending|HeaderInsertRejectedStillWritesPendingHeader|SetBlockBodyNoImplicitCreate|StoredOnlyAfterSuccessfulStore|PruneDeletesPendingAndStored)|TestRestoreBlockTreeLoadsWindowAndCompleteState|TestScanEventsRule5PruneRemovesStoredAndTasks' -count=1
go test ./blocktree -run 'TestInvariant(PruneReturnsRemovedOrphans|OrphansAboveRootAfterPrune)' -count=1
```

审计留痕版（逐条命令与判定标准）见 [形式化验证.md](形式化验证.md) 的第 14 节。