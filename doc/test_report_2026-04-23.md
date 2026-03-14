# Test report (2026-04-23)

> Generated from a clean `go test ./... -count=1` run with `-covermode=atomic` on this date for module **`scanner_eth`**. Packages without tests appear as **0.0%** or **`[no test files]`** in the `go test` output.

**Related:** [README.md](../README.md) · [FormalVerification.md](../FormalVerification.md) · [BlockTree.md](../BlockTree.md) · [design.md](design.md) · [README.md in this folder](README.md) (`doc/` index) · [Historical snapshot (2026-04-13)](test_report_2026-04-13.md)

## 1. Commands

```bash
go test ./... -count=1 -coverprofile=coverage.out -covermode=atomic
go tool cover -func=coverage.out
```

## 2. Package coverage (`go test ./...`)

| Package | Coverage |
|---------|----------|
| `scanner_eth` (main) | 0.0% |
| `scanner_eth/blocktree` | 98.6% |
| `scanner_eth/config` | 0.0% |
| `scanner_eth/data` | no test files |
| `scanner_eth/fetch` | 55.2% |
| `scanner_eth/filter` | 0.0% |
| `scanner_eth/leader` | 53.9% |
| `scanner_eth/log` | 0.0% |
| `scanner_eth/middleware` | 0.0% |
| `scanner_eth/model` | 0.0% |
| `scanner_eth/util` | 100.0% |

## 3. Merged statement total

From `go tool cover -func` on the combined profile:

**Total:** **52.5%** of statements

## 4. Notes

- There is **no** `scanner_eth/publish` package in this module (contrast with the older [test_report_2026-04-13.md](test_report_2026-04-13.md) snapshot).
- To refresh these numbers after code changes, rerun the commands in §1 from the repository root.

## 5. Conclusion

- Full test run **passed** (`exit 0`).
- Heaviest tested code paths: **`fetch`**, **`blocktree`**, **`leader`**, **`util`**.
