# `fetch/` package design

**Related:** [README.md](../README.md) · [BlockTree.md](../BlockTree.md) · [FormalVerification.md](../FormalVerification.md) · [README.md in this folder](README.md) (`doc/` index) · [test_report_2026-04-23.md](test_report_2026-04-23.md) (latest coverage) · [test_report_2026-04-13.md](test_report_2026-04-13.md) (historical)

## 1. Overview

```mermaid
flowchart TD
    A[FetchManager\nfetch_manager.go] --> B[Leader Election\nleader/election.go]
    A --> C[Scan Loop\nstartScanLoop]
    A --> D[HeaderNotifier pool\nheader_notifier.go]
    A --> E[NodeManager\nnode_manager.go]
    A --> F[TaskPool\ntask_pool.go]
    A --> G[BlockTree\nblocktree package]
    A --> PBD[PendingBlockStore\npending_block_store.go]
    A --> H[DB Operator\ndb_operator.go]
    A --> I[BlockFetcher\nfetcher.go + eth_protocol.go]

    D -->|RemoteChainUpdate| A
    E -->|pick best node| I
    I -->|Header| G
    I -->|Header/FullBlock| PBD
    F -->|consume body tasks| J[syncNodeDataByHash\nsync_block_data.go]
    J --> PBD
    J --> E

    C --> K[scanEvents\nscan_flow.go]
    K --> L[getHeaderByHeightSyncTargets]
    K --> M[getHeaderByHashSyncTargets]
    K --> N[getBodySyncTargets]

    L --> O[syncHeaderByHeightTarget]
    M --> PH[syncHeaderByHashTarget]
    N --> Q[syncBodyTarget]

    O --> G
    PH --> G
    Q --> F
    Q --> PBD
    Q --> H

    K --> R[pruneStoredBlocks\nprune_state.go]
    R --> G
    R --> S[storedBlockState\nstored_block_state.go]
```

## 2. Core flow

```mermaid
sequenceDiagram
    participant LE as LeaderElection
    participant FM as FetchManager
    participant HN as HeaderNotifier
    participant NM as NodeManager
    participant SF as scan_flow
    participant BF as BlockFetcher
    participant BT as BlockTree
    participant PBD as PendingBlockStore
    participant TP as TaskPool
    participant DB as DbOperator

    LE->>FM: onBecameLeader
    FM->>FM: resetRuntimeState
    FM->>DB: LoadBlockWindowFromDB(ctx)
    alt DB window non-empty
        FM->>BT: restoreBlockTree
        FM->>PBD: SetBlockHeader(restore headers)
    else DB window empty
        FM->>SF: ensureBootstrapHeader
        SF->>BF: FetchBlockHeaderByHeight
        SF->>BT: insertHeader(topology)
        SF->>PBD: SetBlockHeader
    end

    FM->>TP: start
    FM->>FM: startScanLoop(1s + trigger)

    HN-->>FM: RemoteChainUpdate(header optional)
    FM->>NM: UpdateNodeChainInfo
    opt header present and leader active
        FM->>BT: insertHeader(topology)
        FM->>PBD: SetBlockHeader
    end
    FM->>FM: triggerScan

    FM->>SF: scanEvents
    SF->>SF: getHeaderByHeightSyncTargets
    SF->>SF: getHeaderByHashSyncTargets
    SF->>SF: getBodySyncTargets

    par header by height
        SF->>BF: FetchBlockHeaderByHeight
        SF->>BT: insertHeader(topology)
        SF->>PBD: SetBlockHeader
    and header by hash
        SF->>BF: FetchBlockHeaderByHash
        SF->>BT: insertHeader(topology)
        SF->>PBD: SetBlockHeader
    and body
        SF->>TP: enqueueTaskWithPriority
    end

    TP->>FM: syncNodeDataByHash(task)
    FM->>BF: FetchFullBlock
    FM->>PBD: SetBlockBody / SetBlockHeader
    FM->>FM: triggerScan

    SF->>PBD: GetBlockBody
    SF->>DB: StoreBlockData(ctx)
    SF->>SF: pruneStoredBlocks
    SF->>PBD: DeleteBlockPayload
```

## 3. File responsibilities

- **Scheduling & lifecycle**
  - `fetch_manager.go`: leader callbacks, scan loop, trigger control, runtime reset
  - `scan_flow.go`: target enumeration (height/hash/body), async stage execution, stage logging
- **Nodes & chain head**
  - `node_manager.go`: node readiness, latency, best-node selection
  - `header_notifier.go`: subscribe/poll for new heads and push updates
  - `remote_chain.go`: per-node remote tip height/hash cache
- **Fetch & conversion**
  - `fetcher.go` / `eth_protocol.go`: RPC fetch for header/full block
  - `convert.go` / `cache_erc20.go` / `cache_erc721.go`: conversion and caches
- **Tree & state**
  - `blocktree` package: fork topology, orphan linking, prune, thread-safe API
  - `pending_block_store.go`: `PendingBlockStore`, pending headers/bodies by hash
  - `restore_tree.go`: rebuild tree from DB window
  - `prune_state.go`: prune policy for already-stored blocks
  - `stored_block_state.go`: set of persisted block hashes
- **Task execution**
  - `task_pool.go`: dedupe, priority queue, workers, retries, metrics
  - `sync_block_data.go`: body fetch and write-back to `PendingBlockStore`
- **Storage**
  - `db_operator.go`: DB window read and block persistence
  - `store_worker.go`: batched write pipeline

## 4. Concurrency model

- `BlockTree` is internally locked; callers use its methods only.
- `PendingBlockStore` is owned by `FetchManager`; it maps hash → pending header/body.
- Header sync dedupes along two axes: `headerHeightsSyncing` (by height) and `headerHashesSyncing` (by hash).
- Whether body sync runs is decided by target enumeration plus task-pool dedupe, not a single global boolean.
- `BlockTree.Insert` rule: if `root` is set, headers with height ≤ `root.Height` are rejected.
- Scan is triggered by: periodic ticker (1s), `HeaderNotifier` updates, and `triggerScan` after body sync completes.
