# scanner_eth

Ethereum (and EVM-compatible chain) block scanner: under leader election it maintains a fork-aware block tree, syncs headers and bodies, persists to the database, and notifies via Redis. Supports branch sync, multi-node RPC, remote head notifications, and eventually consistent storage. Main code lives in `fetch/` and `blocktree/`.

Configuration: `config.yaml` at the repository root. Build and test:

```bash
go build ./...
go test ./... -count=1
```

## Documentation

Core design and verification docs now live under `doc/`:

| Document | Description |
|----------|-------------|
| [doc/FormalVerification.md](doc/FormalVerification.md) | Global state model \(S=(T,P,D)\), invariants, audit checklist, and formal/regression test commands |
| [doc/BlockTree.md](doc/BlockTree.md) | `blocktree` package structure, API, branch/prune behavior, and scanner integration |
| [doc/design.md](doc/design.md) | `fetch/` package design, runtime wiring, sequence diagrams, and concurrency model |

Quick ownership shortcut:

- `scan`: derives sync targets from `BlockTree`, backfills missing-body fetch work, builds low-to-high branches, and submits them for persistence
- `task_process`: fetches headers/full blocks, updates `StagingStore`, and can trigger body-fetch work after header insertion
- `serial_store`: serializes branch traversal and DB persistence only; it does not own task-pool orchestration

## Branch Persistence Contract

- `blocktree.Branches()` exposes each branch in leaf-to-root order; `scan` reverses that into low-to-high branch payloads.
- `scan` is responsible for branch materialization and missing-body task backfill on the fetch side; it then hands complete branch payloads to `fetch/serial_store`.
- `fetch/serial_store` is only responsible for serialized branch traversal and DB persistence. It does not own task-pool orchestration.
