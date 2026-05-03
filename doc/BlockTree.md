# Block tree (`BlockTree`)

This document describes the **fork-aware block tree** in the `scanner_eth/blocktree` package: during sync it caches **linked block header** relationships, handles **orphans**, tracks **irreversible ancestor** metadata, and **prunes** when the window grows too large. The structure is guarded by `sync.RWMutex`.

---

## 1. Core types

`BlockTree` itself only stores **header-topology information**. It does not store block body, transaction list, receipt list, or database persistence state. The "block structure" inside this module is therefore the minimal structure needed to maintain a fork-aware header tree.

### 1.1 `Key`

- Actual type: `type Key = string`
- Current meaning: normalized block hash
- Role: unique block identifier used everywhere inside the tree

### 1.2 `Node`

Actual structure:

```go
type Node struct {
    Height    uint64
    Key       Key
    ParentKey Key
    Weight    uint64
}
```

Field meaning:

- `Height`: block height
- `Key`: current block hash
- `ParentKey`: parent block hash
- `Weight`: chain weight used for tie-breaking between competing leaves

This is the minimal in-memory "block header projection" used by `BlockTree`.

### 1.3 `IrreversibleNode`

Actual structure:

```go
type IrreversibleNode struct {
    Height uint64
    Key    Key
}
```

Meaning:

- It records the cached irreversible ancestor for a linked block.
- It is not a separate node stored elsewhere; it is metadata cached on `LinkedNode`.

### 1.4 `LinkedNode`

Actual structure:

```go
type LinkedNode struct {
    Node
    Irreversible IrreversibleNode
}
```

Meaning:

- `Node` is the block's own basic topology data.
- `Irreversible` is the irreversible ancestor computed at insertion time.
- Only **linked** nodes are stored in `keyValue`.
- Orphans are temporarily stored only as plain `Node`, not as `LinkedNode`.

### 1.5 `Branch`

Actual structure:

```go
type Branch struct {
    Header *LinkedNode
    Nodes  []*LinkedNode
}
```

Meaning:

- `Header`: the current leaf of this branch
- `Nodes`: the backward walk from leaf to root
- Order is always **leaf -> parent -> ... -> root**
- Heights inside `Nodes` are non-increasing

Example:

```text
root(a:1) -> b:2 -> c:3
```

then one branch is:

```text
Header = c
Nodes  = [c, b, a]
```

---

## 2. What a "block" means here

In `blocktree`, a block is represented only by these dimensions:

1. **Identity**
   - `Key`
   - `ParentKey`
2. **Order**
   - `Height`
3. **Fork choice bias**
   - `Weight`
4. **Cached ancestry metadata**
   - `Irreversible`

Not included in this module:

- full block body
- transactions
- receipts
- logs
- DB row state
- pending/staging payloads

Those live in `fetch/store`, `fetch/task_process`, and downstream persistence modules.

---

## 3. Internal structure

Actual `BlockTree` fields:

```go
type BlockTree struct {
    mu sync.RWMutex

    keyValue            map[Key]*LinkedNode
    parentToChild       map[Key]map[Key]struct{}
    headers             map[Key]struct{}
    root                *Node
    orphanParentToChild map[Key]map[Key]*Node
    orphanKeySet        map[Key]struct{}
    irreversibleCount   int
}
```

- **`keyValue`**: `Key → *LinkedNode`, nodes linked into the tree.
- **`parentToChild`**: `ParentKey → { ChildKey → struct{} }`, child index.
- **`headers`**: current **leaf** set (linked nodes with no child yet).
- **`root`**: oldest linked node retained in the window (copy of `Node`).
- **`orphanParentToChild`**: orphans whose parent is missing, `ParentKey → { ChildKey → *Node }`.
- **`orphanKeySet`**: all keys in the orphan buffer, for fast membership checks.
- **`irreversibleCount`**: confirmation threshold; pruning is **not** allowed until height gap rules are met (see `Prune`).

---

## 4. Main operations and flows

This section describes not only what each API returns, but also the actual step-by-step flow inside `block_tree.go`.

### 4.1 `Insert(height, key, parentKey, weight) []*LinkedNode`

High-level intent:

- Insert a new block into the linked tree if its parent is known.
- Otherwise buffer it as an orphan.
- If this new block unblocks waiting orphans, cascade them into the tree.

Detailed flow:

1. Acquire write lock.
2. If `key` already exists in `keyValue`:
   - reject
   - return `nil`
3. If `key` already exists in `orphanKeySet`:
   - reject duplicate orphan
   - return `nil`
4. If tree is non-empty and `height <= root.Height`:
   - reject stale/too-low insert
   - return `nil`
5. Check whether `parentKey` exists in `keyValue`.
6. If parent exists, or tree is empty:
   - if tree is empty, initialize `root`
   - call `internalInsert(...)`
   - clone all newly inserted linked nodes
   - return them
7. Otherwise parent is unknown:
   - store the node under `orphanParentToChild[parentKey][key]`
   - record `key` in `orphanKeySet`
   - return `nil`

Important behavioral points:

- `Insert` may return more than one node:
  - the newly inserted node itself
  - plus any orphans that are recursively resolved by this insertion
- `Insert` does **not** call `Prune`
- root initialization happens only on the first linked insert

Example:

```text
Insert(c,parent=b) before b exists -> buffer c as orphan
Insert(b,parent=a) after a exists  -> insert b, then adopt c
return [b, c]
```

### 4.2 `internalInsert(height, key, parentKey, weight)`

This is the real "link into the tree" path.

Detailed flow:

1. Compute `Irreversible` for the new node using `computeIrreversible(parentKey)`.
2. Build a `LinkedNode`.
3. Store it in `keyValue[key]`.
4. Ensure `parentToChild[parentKey]` exists.
5. Add `key` into the parent's child set.
6. Remove `parentKey` from `headers`:
   - parent now has at least one child, so it is no longer a leaf
7. Add `key` into `headers`:
   - new node starts as a leaf
8. Call `checkOrphan(key)`:
   - if some orphans were waiting for this `key`, recursively attach them
9. Return `[newNode] + adoptedOrphans`

### 4.3 Orphan adoption flow: `checkOrphan(key)`

When a parent arrives, `checkOrphan(key)` tries to adopt all children that were waiting on it.

Detailed flow:

1. Look up `orphanParentToChild[key]`.
2. If absent:
   - return `nil`
3. Remove the entire orphan bucket for this parent key.
4. For each child orphan:
   - remove child key from `orphanKeySet`
   - call `internalInsert(...)`
5. Because `internalInsert(...)` itself calls `checkOrphan(...)`, adoption is recursive.

This creates a cascade:

```text
missing parent p
  -> orphan child c1
  -> orphan grandchild c2 waiting on c1

Insert(p)
  -> adopt c1
  -> adopt c2
```

### 4.4 Irreversible ancestor computation: `computeIrreversible(parentKey)`

- From the **new node's parent**, walk `ParentKey` upward `irreversibleCount - 1` steps (the new block is step 0), yielding the irreversible reference.
- If the path is shorter, return the deepest ancestor reachable.
- If `parentKey == ""` and `root` exists, use current root.
- If parent is missing from `keyValue` but `root` exists, also fall back to root.
- If nothing is available, return zero-value `IrreversibleNode{}`.

Detailed flow:

1. If `parentKey == ""`:
   - if root exists, return root as irreversible ancestor
   - else return empty irreversible node
2. If parent exists in `keyValue`:
   - set `current = parent`
   - walk upward up to `irreversibleCount - 1` additional hops
   - stop early if parent chain ends or some ancestor is missing
   - return the oldest reachable node encountered in that walk
3. If parent is not in `keyValue` but root exists:
   - return root
4. Else:
   - return empty irreversible node

Example with `irreversibleCount = 3`:

```text
a(1) <- b(2) <- c(3) <- d(4)
insert e(5), parent=d

new node e is hop 0
parent d is hop 1
c is hop 2
b is hop 3

cached Irreversible = b
```

### 4.5 `Branches() []Branch`

This operation exports the current linked topology as branch snapshots.

Detailed flow:

1. Acquire read lock.
2. Read all current leaf keys from `headers`.
3. Resolve each leaf key to `*LinkedNode`.
4. Sort leaf nodes by:
   - higher `Height` first
   - then higher `Weight`
   - then lexicographically smaller `Key`
5. For each sorted leaf:
   - walk backward using `ParentKey`
   - append each node to `Nodes`
   - stop when parent is absent from `keyValue`
6. Build `Branch{Header, Nodes}` and return all branches.

Important point:

- Branch node order is **leaf-first**, not root-first.
- So a branch often looks like `[tip, parent, grandparent, ..., root]`.
- Downstream `scan` reverses or rematerializes this into low-to-high order before persistence.

### 4.6 `Prune(count uint64) []*LinkedNode`

Pruning is only possible when the height gap between the **tallest leaf** and current `root` is **>= `irreversibleCount`**:

- Among `headers`, pick the leaf with **maximum height**; tie-break by **larger `Weight`** (main-chain bias).
- Target new root height is `bestLeaf.Height - irreversibleCount` (keep as much as constraints allow).
- Delete nodes with height **< targetHeight**, and side branches whose parent was removed and that are **off** the main-chain path.
- Update `root`, prune orphans that are too low (`pruneOrphansAtOrBelow`).
- Return copies of pruned nodes.

`count` constrains the new root: it must be at least `root.Height + count`, intersected with the maximum allowed new-root height; if the interval is empty, return `nil`.

Detailed flow:

1. Acquire write lock.
2. Early-return if:
   - tree is empty
   - no headers exist
   - `count == 0`
3. Find the best leaf from `headers`:
   - larger `Height` wins
   - if same height, larger `Weight` wins
4. Compute `heightDiff = bestLeaf.Height - root.Height`.
5. If `heightDiff < irreversibleCount`:
   - pruning is forbidden
   - return `nil`
6. Compute the allowed new-root interval:
   - `minNewRootHeight = root.Height + count`
   - `maxNewRootHeight = bestLeaf.Height - irreversibleCount`
7. If interval is empty:
   - return `nil`
8. Walk upward from `bestLeaf` using `selectPruneRoot(...)` until a node within that interval is found.
9. Starting from `newRoot`, call `collectDescendants(newRoot.Key, retained)`:
   - this marks the retained subtree under the new root
10. Any linked node not in `retained` becomes prunable.
11. For every prunable linked node:
   - append a cloned copy to return list
   - delete it from `headers`
   - delete it from `keyValue`
   - delete its own `parentToChild` entry
12. Sweep all remaining `parentToChild` maps:
   - remove deleted child references
   - drop empty child sets
13. Replace `root` with `newRoot.Node`
14. Prune orphans with `Height <= root.Height`
15. Append those orphan removals to the return list
16. Return all pruned linked nodes + pruned orphan nodes

Two key design points:

1. The retained set is built from the **new root downward**, not from the old root upward.
2. Side branches whose attachment point is pruned disappear naturally because they are not descendants of the chosen `newRoot`.

### 4.7 Orphan pruning: `pruneOrphansAtOrBelow(threshold)`

This is part of `Prune`, but conceptually separate.

Detailed flow:

1. Iterate every `orphanParentToChild[parentKey]`.
2. For each orphan node:
   - if orphan is `nil`, clean it up
   - if `orphan.Height <= threshold`, remove it
3. For removed orphans:
   - also remove the key from `orphanKeySet`
   - return it as `LinkedNode{Node: *orphanNode}` so prune has a uniform output type
4. Remove empty orphan buckets.

### 4.8 Queries and helpers

| Method | Role |
|--------|------|
| `Get(key)` | Node copy or `nil`. |
| `Root()` | Current root copy or `nil`. |
| `HeightRange()` | `(start, end, ok)`: `start` is root height, `end` is max leaf height. |
| `Branches()` | From each leaf back to root; multiple forks; sort: leaf height desc, weight desc, `Key` asc. |
| `UnlinkedNodes()` | **parentKey** values orphans are waiting for (parent itself not an orphan). |
| `LinkedNodes()` | Copies of all linked nodes. |
| `Len()` | Linked node count (excludes orphan buffer). |

Detailed behavior notes:

- `Get(key)`:
  - only returns **linked** nodes
  - does not expose orphan-only entries
  - always returns a clone, not the internal pointer
- `Root()`:
  - returns a cloned `Node`
  - `nil` when tree is empty
- `HeightRange()`:
  - `start = root.Height`
  - `end = max(header.Height)`
  - returns `ok=false` when tree is empty
- `UnlinkedNodes()`:
  - returns the **missing parent keys** currently blocking orphan adoption
  - filters out parent keys that are themselves orphans
- `LinkedNodes()`:
  - returns snapshots of all linked nodes
- `Len()`:
  - counts only `keyValue`
  - does not count buffered orphans

---

## 5. Relationship to the scanner (`fetch`)

- **`FetchManager`** owns `*blocktree.BlockTree`, inserts headers during scan, derives height/hash sync tasks from leaves and orphans, and prunes when appropriate.
- **Block bodies and storage state** (`StagingStore`, `storedBlocks`, etc.) live in **`fetch`**, not inside `BlockTree`; the tree mainly encodes **header parent/child and fork structure**.
- `Branches()` exposes branches in **leaf -> root** order. The scan flow reverses that slice and constructs low -> high branch payloads before handing them to the serial store worker.
- Missing body fetch work is triggered on the `fetch` side (`task_process` on header insert, plus `scan` backfill for branch nodes that still lack payload), not by the serial store worker.
- The serial store worker then traverses each low -> high branch, requiring the parent block to be already persisted before handling the current node; a parent already pruned out of the retained tree window is treated as an acceptable branch root.
- Actual DB writes are handled in the serial store worker after scan hands off the branch payloads, and a blocked/failed node stops only its own branch.

Finer scan stages and task-pool logic: `fetch/scan/flow.go`, `fetch/taskpool/pool.go`, and [design.md](design.md).

---

## 6. Tests

`blocktree/block_tree_test.go`, `blocktree/invariant_formal_test.go`, etc. cover insert, orphan adoption, pruning, and invariants:

```bash
go test ./blocktree/ -count=1
```

---

## Revision notes

- This file supersedes earlier ad-hoc draft notes (legacy doc name) and matches current `block_tree.go` behavior.
- If behavior changes (especially `Prune` / `Insert` edge cases), update this document and code comments together.

## See also

- [README.md](../README.md): build, configuration, documentation index
- [FormalVerification.md](FormalVerification.md): global state \(S=(T,P,D)\), invariants **T**, formal test commands (§14)
- [design.md](design.md): how `FetchManager`, `BlockTree`, task pool, and pending block store interact
