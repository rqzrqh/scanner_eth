# scanner_eth

Ethereum (and EVM-compatible chain) block scanner: under leader election it maintains a fork-aware block tree, syncs headers and bodies, persists to the database, and notifies via Redis. Supports branch sync, multi-node RPC, remote head notifications, and eventually consistent storage. Main code lives in `fetch/` and `blocktree/`.

Configuration: `config.yaml` at the repository root. Build and test:

```bash
go build ./...
go test ./... -count=1
```

## Documentation

Core docs live under `doc/`:

| Document | Description |
|----------|-------------|
| [doc/FormalVerification.md](doc/FormalVerification.md) | Global state model \(S=(T,P,D)\), invariants, audit checklist, and formal/regression test commands |
| [doc/BlockTree.md](doc/BlockTree.md) | `blocktree` package structure, API, branch/prune behavior, and scanner integration |
| [doc/design.md](doc/design.md) | `fetch/` package design, runtime wiring, sequence diagrams, and concurrency model |
