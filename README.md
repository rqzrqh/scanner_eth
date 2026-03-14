# scanner_eth

Ethereum (and EVM-compatible chain) block scanner: under leader election it maintains a fork-aware block tree, syncs headers and bodies, persists to the database, and notifies via Redis. Supports branch sync, multi-node RPC, remote head notifications, and eventually consistent storage. Main code lives in `fetch/` and `blocktree/`.

Configuration: `config.yaml` at the repository root. Build and test:

```bash
go build ./...
go test ./... -count=1
```

Documentation: [FormalVerification.md](FormalVerification.md) (invariants & test commands), [BlockTree.md](BlockTree.md) (block tree API), [doc/design.md](doc/design.md) (`fetch/` package design), [doc/README.md](doc/README.md) (index). Latest coverage: [doc/test_report_2026-04-23.md](doc/test_report_2026-04-23.md); older snapshot: [doc/test_report_2026-04-13.md](doc/test_report_2026-04-13.md).
