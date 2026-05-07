# scanner_eth

Ethereum (and EVM-compatible chain) block scanner: under leader election it maintains a fork-aware block tree, syncs headers and bodies, persists to the database, and notifies via Redis. Supports branch sync, multi-node RPC, remote head notifications, and eventually consistent storage. Main code lives in `fetch/` and `blocktree/`.

Configuration: `config.yaml` at the repository root. Build and test:

```bash
go build ./...
go test ./... -count=1
```

Run the scanner through the command entrypoint:

```bash
go run . scanner -conf config.yaml -env prd
```

## Log Report

Generate an offline HTML summary from scanner logs:

```bash
go run . reporter -input logs/app.log -output report.html
```

Useful options:

```bash
# Multiple log files, either repeated or comma-separated.
go run . reporter -input logs/app.log -input logs/app-2026-05-04T06-40-43.530.log -output report.html
go run . reporter -input logs/app.log,logs/app-2026-05-04T06-40-43.530.log -output report.html

# Limit the analysis window.
go run . reporter -input logs/app.log -since "2026-05-04 06:40:00" -until "2026-05-04 07:00:00" -output report.html

# Customize the report title.
go run . reporter -input logs/app.log -title "scanner_eth sync report" -output report.html
```

The report includes node request/failure counts, RPC method stats, task pool stats by task kind, scan stage results, store worker stats, runtime sync progress, and anomaly summaries. Open the generated HTML in a browser to review it or print it to PDF.

## Mock Node

`mocknode` starts local HTTP JSON-RPC nodes backed by the scanner database. It is useful for replaying already persisted chain data without calling external RPC providers.

Configure it in `mocknode.conf` at the repository root:

```yaml
base_addr: "127.0.0.1"
base_port: 18545

nodes:
  - latency: 0s
    latency_jitter: 0s
    drop_rate: 0
  - latency: 100ms
    latency_jitter: 50ms
    drop_rate: 0.05

database:
  host: "127.0.0.1"
  port: 3306
  user: root
  password: "123456"
  dbname: mocknode_testnet
  charset: utf8mb4
  parseTime: true
  loc: Local
```

Start the mock nodes in one terminal:

```bash
go run . mocknode --conf mocknode.conf
```

The parent process starts one child process per `nodes` entry on consecutive ports. The list index is the node ID and port offset, for example:

```text
http://127.0.0.1:18545
http://127.0.0.1:18546
```

Child logs are printed by the parent with a node prefix, such as `[mocknode:0 stderr] ...`. When the parent receives a shutdown signal, it terminates all child processes. `chain_id` is read from the database `scanner_info` table, so it does not need to be configured.

The `nodes` section configures per-node network behavior and also determines node count. `latency` adds fixed delay before each request, `latency_jitter` adds a random extra delay from `0` to that duration, and `drop_rate` is a probability from `0` to `1` that closes the request connection without a JSON-RPC response.

### Run Scanner Against Mock Node

Use `mocknode.conf.database` as the mock RPC data source. It should point to a database that already contains the persisted scanner tables to replay, especially `scanner_info`, complete `block` rows, transactions, receipts/log-derived rows, and any optional tables needed by the scanner features you enable.

Use `config.yaml.database` as the scanner runtime database. For a replay sync, point it at a different or empty database so the scanner writes fresh data while mocknode serves historical data from the source database. If both configs point to the same database, the scanner will see the existing stored blocks and resume from that state instead of replaying from scratch.

To make the scanner use these mock nodes, point `fetch.rpc_nodes` in `config.yaml` to the local URLs:

```yaml
fetch:
  rpc_nodes:
    - "http://127.0.0.1:18545"
    - "http://127.0.0.1:18546"
```

Alternatively, start the scanner with `--mocknode`. In this mode `fetch.rpc_nodes` from `config.yaml` is replaced with the local URLs generated from `mocknode.conf`:

```bash
go run . scanner -conf config.yaml -env prd --mocknode
```

Keep the `mocknode` process running while the scanner is syncing. The scanner still reads all non-RPC settings from `config.yaml`, including chain metadata, Redis, store options, task pool settings, and metrics. The chain ID and genesis hash in `config.yaml` must match the `scanner_info` row in the mocknode source database.

## Documentation

Core docs live under `doc/`:

| Document | Description |
|----------|-------------|
| [doc/FormalVerification.md](doc/FormalVerification.md) | Global state model \(S=(T,P,D)\), invariants, audit checklist, and formal/regression test commands |
| [doc/BlockTree.md](doc/BlockTree.md) | `blocktree` package structure, API, branch/prune behavior, and scanner integration |
| [doc/design.md](doc/design.md) | `fetch/` package design, runtime wiring, sequence diagrams, and concurrency model |
