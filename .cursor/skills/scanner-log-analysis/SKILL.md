---
name: scanner-log-analysis
description: Analyze scanner_eth synchronization logs for sync success rate, duplicate writes, node load, retry behavior, task backlog, storage throughput, and sync progress. Use when the user asks to analyze logs, diagnose sync health, compare node performance, investigate duplicate writes, or summarize scanner runtime status.
---

# Scanner Log Analysis

## Scope

Use this skill for `scanner_eth` runtime log analysis, especially fetch/task/store/scan behavior. Focus on evidence from logs and metrics payloads, not guesses.

Primary questions this skill should answer:

- Sync success rate: task, scan stage, body fetch, header fetch, and store branch success/failure.
- Duplicate writes: repeated store attempts, DB duplicate-key errors, repeated block persistence, or repeated pending body/header writes.
- Node load and health: per-node RPC usage, failures, latency, ready/unready transitions, and retry distribution.
- Sync progress: latest remote height, block tree progress, stored count, task backlog, stage duration, and lag.
- Retry behavior: taskpool retries versus per-RPC retries inside body/full-block fetch.
- Pipeline bottleneck: where progress stops across `BlockTree`, `StagingStore`, `TaskPool`, `FetchFullBlock`, `serial_store`, and DB persistence.
- Fork/reorg health: branch growth, same-height competing hashes, irreversible window behavior, and whether branches converge.

## Inputs

Ask for log file paths if none are obvious. Prefer real log files, terminal output, or copied log snippets. If the user provides a directory, search within it using `rg`, not broad shell grep.

Useful patterns in this repo:

- `scan stage event stage:` for scan stage status and durations.
- `task pool stats` for enqueue/dequeue/success/failure/retry/drop/backlog.
- `store block worker stats` for persistence throughput and skip/failure reasons.
- `runtime health stats` for periodic end-to-end progress and backlog snapshots.
- `valid node operators selected` for body-sync candidate node selection.
- `body sync start`, `body sync success`, and `body sync failed` for per-block body fetch status.
- `fetch full block rpc failed` and `fetch full block rpc exhausted retries` for per-RPC retry health.
- `fetch full block rpc success` for per-RPC selected nodes, attempts, and latency.
- `fetch .* failed` with `nodeId:` for node-specific RPC failures.
- `fetch .* success` with `nodeId:` and `cost:` for node latency and load.
- `store block. height:` for DB persistence row counts, task counts, and latency.
- `MetricsPayload`, `/debug/vars`, or expvar snapshots for structured runtime state.

## Workflow

1. Identify the analysis window:
   - Time range or log file boundaries.
   - Chain, environment, node count, start height, and target/latest height if available.
   - Whether logs include all workers or only one process.

2. Extract high-signal events:
   - Scan stages: count success/failure by stage, target count, duration, error message.
   - Taskpool stats: body/header enqueued, succeeded, failed, retried, dropped, pending, tracked.
   - Store stats: submitted, skipped, succeeded, failed, canceled, skipped_missing_body, skipped_parent_not_ready, failed_db.
   - RPC events: per node operation count, failure count, retry count, latency/cost.
   - Runtime snapshots: latest height, blocktree root/leaves, stored count, node ready count.
   - Block lineage events: inserted headers, branch targets, store branches, pruning, remote header candidates.

3. Compute derived metrics when enough data exists:
   - Sync success rate = succeeded / (succeeded + failed), by stage and task kind.
   - Retry pressure = retried / dequeued or per-RPC retry failures / RPC attempts.
   - Drop rate = dropped / enqueued.
   - Store success rate = succeeded / submitted.
   - Missing-body ratio = skipped_missing_body / skipped.
   - Parent-not-ready ratio = skipped_parent_not_ready / skipped.
   - Node failure rate = node failures / node RPC attempts.
   - Node load share = node RPC attempts / total RPC attempts.
   - Progress speed = height delta / time delta, if heights and timestamps exist.
   - Lag = remote latest height - stored or best local height, if both exist.
   - Pipeline lag: remote latest -> blocktree latest -> pending body/store -> DB stored latest.
   - Method failure rate: failures per RPC method / attempts per RPC method.

4. Look for failure signatures:
   - Repeated `fetch full block rpc exhausted retries` on the same op means node pool or RPC method quality issue.
   - High `skipped_missing_body` means body fetch is behind store branch submission.
   - High `skipped_parent_not_ready` means branches are arriving before ancestors are persisted.
   - High `dropped` task count means queue sizing or worker throughput is insufficient.
   - One node with disproportionate failures or latency should be marked as suspect.
   - Balanced high latency across nodes usually indicates upstream/network pressure, not one bad node.
   - Duplicate-key DB errors or repeated successful store of the same hash indicate duplicate write risk.

## Project-Specific Checks

Use this checklist for `scanner_eth` before making conclusions:

- End-to-end stage split:
  - Header fetched but not inserted into `BlockTree`: inspect header fetch and parent/header validation.
  - Header inserted but body missing: inspect body tasks, `FetchFullBlock`, and per-RPC retries.
  - Body pending but not stored: inspect store branch submission, parent readiness, and `serial_store` skips.
  - Store submitted but DB not updated: inspect DB errors, duplicate writes, deadlocks, and failed_db.

- Fork and branch health:
  - Compare same-height hashes and branch targets to detect active forks or noisy nodes.
  - Check whether branch count and leaf count keep growing without pruning or persistence.
  - Verify `irreversible` pruning only removes safely persisted or stale branch data.
  - Treat long-lived competing branches near the tip differently from deep branch divergence.

- Task scheduling and dedupe:
  - Repeated body tasks for the same hash may be normal after retry, but persistent repeats suggest body fetch or staging failure.
  - Repeated header-by-hash for the same target suggests missing ancestors or remote header churn.
  - High `tracked` with low pending queue can mean tasks are stuck in-flight.
  - High `dropped` means queue pressure; compare with worker count and node count.

- RPC method bottlenecks:
  - `FetchTransactionsByHashBatch`: transaction lookup throughput or missing transaction data.
  - `FetchReceiptsBatch`: receipt RPC slowness, missing receipts, or block not fully indexed by the node.
  - `FetchInternalTxTracesByBlockHash`: trace node capacity and debug API availability.
  - `FetchBalanceNative`, `FetchErc20BalancesBatch`, `FetchErc1155BalancesBatch`: state query pressure.
  - `FetchContractErc20`, `FetchContractErc721`, `FetchTokenErc721`: cache miss pressure and contract/token metadata hotspots.

- Node selection and load:
  - Check if one node handles most calls because it has the lowest delay.
  - Check if failures cluster by node, RPC method, or height range.
  - Check if nodes are excluded because remote height is behind the target height.
  - Check whether failed nodes recover and rejoin later requests.
  - For per-RPC retries, verify retry attempts use different node IDs when enough valid nodes exist.

- DB and persistence quality:
  - Duplicate-key errors imply idempotency or duplicate scheduling questions; repeated successful writes imply a stronger duplicate write issue.
  - Deadlock or lock wait timeout means DB contention; compare with store worker throughput and branch size.
  - High `skipped_parent_not_ready` means ancestor persistence is lagging body availability.
  - High `skipped_missing_body` means branch materialization is ahead of body fetch.

- Cache effects:
  - Contract cache misses can make full-block latency spike on blocks with many new token contracts.
  - ERC721 token metadata calls can dominate blocks with many NFT transfers.
  - A sudden rise in metadata RPC failures may look like body fetch failure even when tx and receipt fetch are healthy.

- Error classification:
  - Network timeout, context deadline, EOF, and connection reset are usually transient.
  - Node height too low or missing receipts may indicate lagging or non-archive/indexing limitations.
  - `execution reverted`, invalid opcode, or similar state-call errors may be non-retryable depending on the call.
  - Decode/parse errors are data quality or parser compatibility issues, not node load alone.

## Output Format

Use this structure unless the user asks for a different format:

```markdown
## 结论
[1-3 句概括同步健康度、主要瓶颈、是否需要人工处理。]

## 关键指标
- 同步成功率: ...
- 重试/失败: ...
- 重复写入: ...
- 节点负载: ...
- 同步进度: ...
- 链路瓶颈: ...

## 发现的问题
- [严重程度] 问题: 证据、影响、建议。

## 建议动作
1. ...
2. ...
```

## Evidence Rules

- Quote exact log snippets only when they are short and decisive.
- Always distinguish observed facts from inference.
- If logs are incomplete, state the missing data and avoid pretending to know rates.
- Prefer tables for node comparison and stage metrics.
- Mention the exact log files or command outputs used, but do not invent paths.

## Useful Searches

Use targeted searches like:

```bash
rg "scan stage event stage:" <log-path>
rg "task pool stats" <log-path>
rg "store block worker stats" <log-path>
rg "runtime health stats" <log-path>
rg "valid node operators selected|body sync (start|success|failed)" <log-path>
rg "fetch full block rpc (failed|exhausted retries)" <log-path>
rg "fetch full block rpc success" <log-path>
rg "store block\\. height:" <log-path>
rg "nodeId:" <log-path>
rg "duplicate|Duplicate|Error 1062|duplicat" <log-path>
rg "missing_body|skipped_missing_body|parent_not_ready|skipped_parent_not_ready" <log-path>
rg "FetchTransactionsByHashBatch|FetchReceiptsBatch|FetchInternalTxTracesByBlockHash" <log-path>
rg "FetchBalanceNative|FetchErc20BalancesBatch|FetchErc1155BalancesBatch|FetchContract|FetchToken" <log-path>
rg "branch|leaf|prune|irreversible|remote header" <log-path>
```
