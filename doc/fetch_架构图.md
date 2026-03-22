# fetch 目录整体架构图

## 1. 总览

```mermaid
flowchart TD
    A[FetchManager\nfetch_manager.go] --> B[Leader Election\nleader/election.go]
    A --> C[Scan Loop\nstartScanLoop]
    A --> D[HeaderNotifier 集群\nheader_notifier.go]
    A --> E[NodeManager\nnode_manager.go]
    A --> F[TaskPool\ntask_pool.go]
    A --> G[BlockTree\nblocktree package]
    A --> PBD[BlockPayloadStore\npayload_store.go]
    A --> H[DB Operator\ndb_operator.go]
    A --> I[BlockFetcher\nfetcher.go + eth_protocol.go]

    D -->|RemoteChainUpdate| A
    E -->|选最佳节点| I
    I -->|Header| G
    I -->|Header/FullBlock| PBD
    F -->|消费 body 任务| J[syncNodeDataByHash\nsync_block_data.go]
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

## 2. 核心流程

```mermaid
sequenceDiagram
    participant LE as LeaderElection
    participant FM as FetchManager
    participant HN as HeaderNotifier
    participant NM as NodeManager
    participant SF as scan_flow
    participant BF as BlockFetcher
    participant BT as BlockTree
  participant PBD as BlockPayloadStore
    participant TP as TaskPool
    participant DB as DbOperator

    LE->>FM: onBecameLeader
    FM->>FM: resetRuntimeState
    FM->>DB: LoadBlockWindowFromDB
    alt DB有窗口数据
        FM->>BT: restoreBlockTree
      FM->>PBD: SetBlockHeader(restore headers)
    else DB为空
        FM->>SF: ensureBootstrapHeader
        SF->>BF: FetchBlockHeaderByHeight
      SF->>BT: insertHeader(拓扑)
      SF->>PBD: SetBlockHeader
    end

    FM->>TP: start
    FM->>FM: startScanLoop(1s + trigger)

    HN-->>FM: RemoteChainUpdate(header可选)
    FM->>NM: UpdateNodeChainInfo
    opt 带header且leader激活
      FM->>BT: insertHeader(拓扑)
      FM->>PBD: SetBlockHeader
    end
    FM->>FM: triggerScan

    FM->>SF: scanEvents
    SF->>SF: getHeaderByHeightSyncTargets
    SF->>SF: getHeaderByHashSyncTargets
    SF->>SF: getBodySyncTargets

    par header by height
        SF->>BF: FetchBlockHeaderByHeight
      SF->>BT: insertHeader(拓扑)
      SF->>PBD: SetBlockHeader
    and header by hash
        SF->>BF: FetchBlockHeaderByHash
      SF->>BT: insertHeader(拓扑)
      SF->>PBD: SetBlockHeader
    and body
        SF->>TP: enqueueTaskWithPriority
    end

    TP->>FM: syncNodeDataByHash(task)
    FM->>BF: FetchFullBlock
    FM->>PBD: SetBlockBody / SetBlockHeader
    FM->>FM: triggerScan

    SF->>PBD: GetBlockBody
    SF->>DB: StoreBlockData
    SF->>SF: pruneStoredBlocks
    SF->>PBD: DeleteBlockPayload
```

## 3. 目录内职责映射

- 调度与生命周期
  - fetch_manager.go: Leader 回调、扫描循环、触发控制、运行时重置
  - scan_flow.go: 目标枚举（height/hash/body）、异步 stage 执行、阶段日志
- 节点与链头
  - node_manager.go: 节点可用性、延迟、最佳节点选择
  - header_notifier.go: 订阅/轮询新区块头并推送更新
  - remote_chain.go: 单节点远端链高度/哈希缓存
- 数据抓取与转换
  - fetcher.go / eth_protocol.go: RPC 协议抓取 header/full block
  - convert.go / cache_erc20.go / cache_erc721.go: 转换与缓存
- 树与状态
  - blocktree package: 仅负责分叉拓扑、孤块挂接、修剪、线程安全
  - payload_store.go: BlockPayloadStore，按 hash 缓存区块头/区块体
  - restore_tree.go: DB窗口恢复到树
  - prune_state.go: 已存储块裁剪策略
  - stored_block_state.go: 已落库 hash 集
- 任务执行
  - task_pool.go: 任务去重、优先级队列、worker、重试、指标
  - sync_block_data.go: body 拉取与写回 BlockPayloadStore
- 存储
  - db_operator.go: DB窗口读取与块数据落库
  - store_worker.go: 数据写入工作流

## 4. 当前并发模型

- BlockTree 内部加锁，外部通过方法访问。
- BlockPayloadStore 由 FetchManager 持有，按 hash 管理区块头/区块体。
- Header 同步按维度去重：
  - 高度维度: headerHeightsSyncing
  - hash 维度: headerHashesSyncing
- Body 是否执行通过目标枚举 + 任务池去重判断，不依赖单一布尔状态。
- BlockTree 插入约束：当 root 非空时，不接受高度小于等于 root.Height 的数据。
- 触发源有三类：
  - 定时 ticker(1s)
  - HeaderNotifier 到达
  - body 同步完成后 triggerScan
