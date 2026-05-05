---
name: scanner-log-analysis
description: Analyze scanner_eth synchronization logs for sync success rate, duplicate writes, node load, retry behavior, task backlog, storage throughput, and sync progress. Use when the user asks to analyze logs, diagnose sync health, compare node performance, investigate duplicate writes, or summarize scanner runtime status.
---

# Scanner Log Analysis

## Scope

Use this skill for `scanner_eth` runtime log analysis, especially fetch/task/store/scan behavior. Prefer the coded report generator over manual parsing so that metric definitions stay centralized in the repository.

## Inputs

By default, look for runtime logs under the repository root `logs/` directory. Ask for log file paths only if `logs/` is missing, empty, or the user wants a specific external log source. Prefer real log files, terminal output, or copied log snippets. If the user provides a directory, search within it using `rg`, not broad shell grep.

Important: this repository ignores `logs` in `.gitignore`, so file discovery tools that respect ignore rules may not show `logs/app.log` even when it exists. Always check the default path directly first, for example with `ls -la logs logs/app.log` from the repository root, before asking the user for a log path.

## Workflow

1. Identify the log files and time window.
2. Run the built-in report generator from the repository root:

```bash
go run ./cmd/logreport -input <log-path> -output report.html
```

For multiple files or a time window:

```bash
go run ./cmd/logreport \
  -input logs/app.log,logs/app-2026-05-04T06-40-43.530.log \
  -since "2026-05-04 06:40:00" \
  -until "2026-05-04 07:00:00" \
  -output report.html
```

3. Use the generated HTML as the primary evidence source. It computes node load/failures, RPC method health, task pool stats by kind, body sync stats, node candidate selection, scan stages, store worker stats, runtime progress, and anomaly summaries. The sync progress section must include the visual progress chart so users can see `remote_latest`, `stored_count`, and `blocktree_end` moving over time instead of relying only on a table.
4. Only do targeted manual searches when the report shows missing data, a suspicious anomaly, or the user asks for deeper root-cause evidence.

## Analysis Model

The analysis should first summarize each module independently, then explain the likely cause by comparing those module signals with overall block progress.

- Treat sync progress as the backbone of the diagnosis. Use `remote_latest`, `blocktree_end`, `stored_height`, `stored_count`, and lag trends to decide whether the scanner is advancing, stalled before body fetch, stalled before storage, or falling behind the chain tip.
- Break the pipeline into modules: node selection, header fetch, body/full-block RPC, task pool, scan stages, serial store, DB writes, and runtime health. Each module should have its own counters, latency, failure, retry, backlog, and throughput signals when the logs provide them.
- Interpret module metrics in the context of progress. High RPC latency matters most when block progress slows; DB write cost matters most when fetched bodies accumulate but stored height or stored count does not advance.
- Avoid diagnosing from one metric alone. A root cause should connect a local symptom, such as node failures or store queue growth, to an end-to-end progress symptom, such as widening lag or flat stored height.
- Prefer cause chains over isolated observations. For example: node height lag reduces valid nodes, which increases body RPC retries, which slows body completion, which leaves store input low and keeps stored height flat.

## Project-Specific Checks

The report generator covers the common checks below. Use them as manual follow-up prompts when the generated report is not enough:

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

When responding in chat after generating or inspecting a report, use this structure unless the user asks for a different format:

```markdown
## Conclusion
[Summarize sync health, the main bottleneck, and whether manual intervention is needed in 1-3 sentences.]

## Key Metrics
- Sync success rate: ...
- Retries/failures: ...
- Duplicate writes: ...
- Node load: ...
- Sync progress: ...
- Pipeline bottleneck: ...

## Findings
- [Severity] Issue: evidence, impact, recommendation.

## Recommended Actions
1. ...
2. ...
```

## Evidence Rules

- Quote exact log snippets only when they are short and decisive.
- Always distinguish observed facts from inference.
- If logs are incomplete, state the missing data and avoid pretending to know rates.
- Prefer tables for node comparison and stage metrics.
- Use the HTML progress chart when discussing sync advancement; mention whether the visual trend shows forward movement, stalled storage, or widening lag.
- Mention the exact log files or command outputs used, but do not invent paths.

## Useful Searches

Use targeted searches only as a fallback or for drilling into report findings:

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
