# Block tree (`BlockTree`)

This document describes the **fork-aware block tree** in the `scanner_eth/blocktree` package: during sync it caches **linked block header** relationships, handles **orphans**, tracks **irreversible ancestor** metadata, and **prunes** when the window grows too large. The structure is guarded by `sync.RWMutex`.

---

## 1. Core types

| Type | Description |
|------|-------------|
| `Key` | Block identifier; currently `string` (usually a normalized block hash). |
| `Node` | `Height`, `Key`, `ParentKey`, `Weight` — basic in-tree data for a block. |
| `IrreversibleNode` | `Height`, `Key` — the **irreversible ancestor** for a block (see below). |
| `LinkedNode` | Embeds `Node` and caches `Irreversible`. |
| `Branch` | From a leaf (header) walk `ParentKey` back to the root; `Nodes` order is **leaf → parent → … → root** (heights high to low). |

---

## 2. Internal structure (conceptual)

- **`keyValue`**: `Key → *LinkedNode`, nodes linked into the tree.
- **`parentToChild`**: `ParentKey → { ChildKey → struct{} }`, child index.
- **`headers`**: current **leaf** set (linked nodes with no child yet).
- **`root`**: oldest linked node retained in the window (copy of `Node`).
- **`orphanParentToChild`**: orphans whose parent is missing, `ParentKey → { ChildKey → *Node }`.
- **`orphanKeySet`**: all keys in the orphan buffer, for fast membership checks.
- **`irreversibleCount`**: confirmation threshold; pruning is **not** allowed until height gap rules are met (see `Prune`).

---

## 3. Main API

### 3.1 `Insert(height, key, parentKey, weight, irreversible) []*LinkedNode`

- If `key` is already in the tree or in the orphan set → returns `nil`.
- If `height <= root.Height` (tree non-empty) → reject insert, return `nil`.
- **Parent already in tree or tree is empty**: establish first root if needed, `internalInsert`, **cascade adoption** of orphans that waited on this `key`; returns list of newly inserted `LinkedNode` copies.
- **Parent unknown**: enqueue as orphan, return `nil`.

`irreversible` is optional context for `computeIrreversible` when the parent cannot be resolved yet.

### 3.2 Irreversible ancestor `computeIrreversible`

- From the **new node’s parent**, walk `ParentKey` upward `irreversibleCount - 1` steps (the new block is step 0), yielding the irreversible reference.
- If the path is shorter, return the deepest ancestor reachable.
- Edge cases with `parentKey == ""` combine `root` and the `irreversible` argument.

### 3.3 `Prune(count uint64) []*LinkedNode`

Pruning is only possible when the height gap between the **tallest leaf** and current `root` is **≥ `irreversibleCount`**:

- Among `headers`, pick the leaf with **maximum height**; tie-break by **larger `Weight`** (main-chain bias).
- Target new root height is `bestLeaf.Height - irreversibleCount` (keep as much as constraints allow).
- Delete nodes with height **< targetHeight**, and side branches whose parent was removed and that are **off** the main-chain path.
- Update `root`, prune orphans that are too low (`pruneOrphansAtOrBelow`).
- Return copies of pruned nodes.

`count` constrains the new root: it must be at least `root.Height + count`, intersected with the maximum allowed new-root height; if the interval is empty, return `nil`.

### 3.4 Queries and helpers

| Method | Role |
|--------|------|
| `Get(key)` | Node copy or `nil`. |
| `Root()` | Current root copy or `nil`. |
| `HeightRange()` | `(start, end, ok)`: `start` is root height, `end` is max leaf height. |
| `Branches()` | From each leaf back to root; multiple forks; sort: leaf height desc, weight desc, `Key` asc. |
| `UnlinkedNodes()` | **parentKey** values orphans are waiting for (parent itself not an orphan). |
| `LinkedNodes()` | Copies of all linked nodes. |
| `Len()` | Linked node count (excludes orphan buffer). |

---

## 4. Relationship to the scanner (`fetch`)

- **`FetchManager`** owns `*blocktree.BlockTree`, inserts headers during scan, derives height/hash sync tasks from leaves and orphans, and prunes when appropriate.
- **Block bodies and storage state** (`pendingBlockStore`, `storedBlocks`, etc.) live in **`fetch`**, not inside `BlockTree`; the tree mainly encodes **header parent/child and fork structure**.

Finer scan stages and task-pool logic: `fetch/scan_flow.go` and [doc/design.md](doc/design.md).

---

## 5. Tests

`blocktree/block_tree_test.go`, `blocktree/invariant_formal_test.go`, etc. cover insert, orphan adoption, pruning, and invariants:

```bash
go test ./blocktree/ -count=1
```

---

## Revision notes

- This file supersedes earlier ad-hoc draft notes (legacy doc name) and matches current `block_tree.go` behavior.
- If behavior changes (especially `Prune` / `Insert` edge cases), update this document and code comments together.

## See also

- [README.md](README.md): build, configuration, documentation index
- [FormalVerification.md](FormalVerification.md): global state \(S=(T,P,D)\), invariants **T**, formal test commands (§14)
- [doc/design.md](doc/design.md): how `FetchManager`, `BlockTree`, task pool, and pending block store interact
- [doc/README.md](doc/README.md): index under `doc/`
- [doc/test_report_2026-04-23.md](doc/test_report_2026-04-23.md): latest coverage snapshot
- [doc/test_report_2026-04-13.md](doc/test_report_2026-04-13.md): historical coverage snapshot
