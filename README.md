# scanner_eth

Ethereum (and EVM-compatible chain) block scanner: under leader election it maintains a fork-aware block tree, syncs headers and bodies, persists to the database, and notifies via Redis. Supports branch sync, multi-node RPC, remote head notifications, and eventually consistent storage. Main code lives in `fetch/` and `blocktree/`.

Configuration: `config.yaml` at the repository root. Build and test:

```bash
go build ./...
go test ./... -count=1
```

## Log Report

Generate an offline HTML summary from scanner logs:

```bash
go run ./cmd/logreport -input logs/app.log -output report.html
```

Useful options:

```bash
# Multiple log files, either repeated or comma-separated.
go run ./cmd/logreport -input logs/app.log -input logs/app-2026-05-04T06-40-43.530.log -output report.html
go run ./cmd/logreport -input logs/app.log,logs/app-2026-05-04T06-40-43.530.log -output report.html

# Limit the analysis window.
go run ./cmd/logreport -input logs/app.log -since "2026-05-04 06:40:00" -until "2026-05-04 07:00:00" -output report.html

# Customize the report title.
go run ./cmd/logreport -input logs/app.log -title "scanner_eth sync report" -output report.html
```

The report includes node request/failure counts, RPC method stats, task pool stats by task kind, scan stage results, store worker stats, runtime sync progress, and anomaly summaries. Open the generated HTML in a browser to review it or print it to PDF.

## Documentation

Core docs live under `doc/`:

| Document | Description |
|----------|-------------|
| [doc/FormalVerification.md](doc/FormalVerification.md) | Global state model \(S=(T,P,D)\), invariants, audit checklist, and formal/regression test commands |
| [doc/BlockTree.md](doc/BlockTree.md) | `blocktree` package structure, API, branch/prune behavior, and scanner integration |
| [doc/design.md](doc/design.md) | `fetch/` package design, runtime wiring, sequence diagrams, and concurrency model |
