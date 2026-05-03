package blocktree

import (
	"sort"
	"sync"
)

// Key is the identifier type for tree nodes.
// Declared as a type alias so it can be swapped without touching other code.
type Key = string

// Node holds the core fields of a block.
type Node struct {
	Height    uint64
	Key       Key
	ParentKey Key
	Weight    uint64
}

// IrreversibleNode represents the irreversible ancestor of a block.
type IrreversibleNode struct {
	Height uint64
	Key    Key
}

// LinkedNode extends Node with cached irreversibility information.
type LinkedNode struct {
	Node
	Irreversible IrreversibleNode
}

// Branch represents one chain branch from a leaf header back toward the linked root.
// Nodes are ordered leaf-first: Nodes[0] is the header (leaf), then ParentKey hops toward
// the root, so heights are non-increasing along the slice.
type Branch struct {
	Header *LinkedNode
	Nodes  []*LinkedNode
}

func cloneLinkedNode(n *LinkedNode) *LinkedNode {
	if n == nil {
		return nil
	}
	v := *n
	return &v
}

// BlockTree is a fork-aware block tree. Blocks whose parent is not yet known
// are buffered as orphans and adopted once the parent arrives.
//
// Safe for concurrent use.
type BlockTree struct {
	mu sync.RWMutex

	// keyValue maps a block key to its LinkedNode.
	keyValue map[Key]*LinkedNode

	// parentToChild maps a parent key to the set of its direct children.
	parentToChild map[Key]map[Key]struct{}

	// headers is the set of leaf keys (nodes with no children yet).
	headers map[Key]struct{}

	// root is the oldest retained block. nil when the tree is empty.
	root *Node

	// orphanParentToChild stores blocks whose parent has not been seen yet.
	// Indexed as: parentKey -> childKey -> Node.
	orphanParentToChild map[Key]map[Key]*Node

	// orphanKeySet records all keys currently buffered in orphanParentToChild.
	orphanKeySet map[Key]struct{}

	// irreversibleCount is the number of confirmations before a block is
	// considered irreversible.
	irreversibleCount int
}

// NewBlockTree creates an empty BlockTree.
func NewBlockTree(irreversibleCount int) *BlockTree {
	return &BlockTree{
		keyValue:            make(map[Key]*LinkedNode),
		parentToChild:       make(map[Key]map[Key]struct{}),
		headers:             make(map[Key]struct{}),
		orphanParentToChild: make(map[Key]map[Key]*Node),
		orphanKeySet:        make(map[Key]struct{}),
		irreversibleCount:   irreversibleCount,
	}
}

// Insert adds a block to the tree. If the parent is unknown the block is
// buffered as an orphan and adopted automatically when its parent arrives.
//
// Returns the list of LinkedNodes inserted during this call (the block itself
// plus any cascadingly resolved orphans). Returns nil if the block already
// existed or was buffered as an orphan.
func (t *BlockTree) Insert(height uint64, key, parentKey Key, weight uint64) []*LinkedNode {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.keyValue[key]; exists {
		return nil
	}
	if _, exists := t.orphanKeySet[key]; exists {
		return nil
	}
	if t.root != nil && height <= t.root.Height {
		return nil
	}

	_, parentExists := t.keyValue[parentKey]

	if parentExists || t.root == nil {
		if t.root == nil {
			t.root = &Node{Height: height, Key: key, ParentKey: parentKey, Weight: weight}
		}
		inserted := t.internalInsert(height, key, parentKey, weight)
		result := make([]*LinkedNode, 0, len(inserted))
		for _, nv := range inserted {
			result = append(result, cloneLinkedNode(nv))
		}
		return result
	}

	// Parent unknown: buffer as orphan.
	if t.orphanParentToChild[parentKey] == nil {
		t.orphanParentToChild[parentKey] = make(map[Key]*Node)
	}
	t.orphanParentToChild[parentKey][key] = &Node{Height: height, Key: key, ParentKey: parentKey, Weight: weight}
	t.orphanKeySet[key] = struct{}{}
	return nil
}

// internalInsert links the block into the tree and resolves orphans waiting
// on this block. Does NOT call prune.
func (t *BlockTree) internalInsert(height uint64, key, parentKey Key, weight uint64) []*LinkedNode {
	perfectIrr := t.computeIrreversible(parentKey)

	nv := &LinkedNode{
		Node:         Node{Height: height, Key: key, ParentKey: parentKey, Weight: weight},
		Irreversible: perfectIrr,
	}
	t.keyValue[key] = nv

	if t.parentToChild[parentKey] == nil {
		t.parentToChild[parentKey] = make(map[Key]struct{})
	}
	t.parentToChild[parentKey][key] = struct{}{}

	// The parent now has a child, so it is no longer a leaf.
	delete(t.headers, parentKey)
	// The new block has no children yet; it is a leaf.
	t.headers[key] = struct{}{}

	result := []*LinkedNode{nv}
	result = append(result, t.checkOrphan(key)...)
	return result
}

// computeIrreversible returns the irreversible ancestor of a node whose
// direct parent has key parentKey.
//
// The new node counts as hop 0, so the walk starts at parentKey (hop 1) and
// continues for irreversibleCount-1 more hops.
//
// If the required ancestor depth is not available, returns the first
// (oldest reachable) node on this path.
func (t *BlockTree) computeIrreversible(parentKey Key) IrreversibleNode {
	if parentKey == "" {
		if t.root != nil {
			return IrreversibleNode{Height: t.root.Height, Key: t.root.Key}
		}
		return IrreversibleNode{}
	}

	if parent, ok := t.keyValue[parentKey]; ok {
		first := parent
		current := parent
		for i := 1; i < t.irreversibleCount; i++ {
			if current.ParentKey == "" {
				return IrreversibleNode{Height: first.Height, Key: first.Key}
			}
			next, ok := t.keyValue[current.ParentKey]
			if !ok {
				return IrreversibleNode{Height: first.Height, Key: first.Key}
			}
			current = next
			first = current
		}
		return IrreversibleNode{Height: first.Height, Key: first.Key}
	}

	if t.root != nil {
		return IrreversibleNode{Height: t.root.Height, Key: t.root.Key}
	}

	return IrreversibleNode{}
}

// checkOrphan resolves orphans whose parent key matches key, inserting them
// via internalInsert and cascading until no more orphans can be resolved.
func (t *BlockTree) checkOrphan(key Key) []*LinkedNode {
	siblings, ok := t.orphanParentToChild[key]
	if !ok {
		return nil
	}
	delete(t.orphanParentToChild, key)

	var result []*LinkedNode
	for _, v := range siblings {
		delete(t.orphanKeySet, v.Key)
		result = append(result, t.internalInsert(v.Height, v.Key, v.ParentKey, v.Weight)...)
	}
	return result
}

// Prune removes blocks from root up to count heights and all their branch nodes.
// The height difference between the longest chain and root must be at least irreversibleCount.
// Returns the list of pruned nodes.
func (t *BlockTree) Prune(count uint64) []*LinkedNode {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.root == nil || len(t.headers) == 0 || count == 0 {
		return nil
	}

	// Find the best leaf: greatest height, ties broken by heaviest weight.
	var bestNV *LinkedNode
	for k := range t.headers {
		nv := t.keyValue[k]
		if nv == nil {
			continue
		}
		if bestNV == nil ||
			nv.Height > bestNV.Height ||
			(nv.Height == bestNV.Height && nv.Weight > bestNV.Weight) {
			bestNV = nv
		}
	}
	if bestNV == nil {
		return nil
	}

	// Height difference between longest chain and root must be at least irreversibleCount.
	heightDiff := bestNV.Height - t.root.Height
	if int(heightDiff) < t.irreversibleCount {
		return nil
	}

	// Calculate the minimum and maximum possible new root heights:
	// - Minimum: current root height + count
	// - Maximum: highest height that maintains irreversible count
	minNewRootHeight := t.root.Height + count
	maxNewRootHeight := bestNV.Height - uint64(t.irreversibleCount)

	// The new root height must satisfy: minNewRootHeight <= newRoot.Height <= maxNewRootHeight
	if minNewRootHeight > maxNewRootHeight {
		return nil
	}

	newRoot := t.selectPruneRoot(bestNV, minNewRootHeight, maxNewRootHeight)
	if newRoot == nil {
		return nil
	}

	retained := make(map[Key]struct{})
	t.collectDescendants(newRoot.Key, retained)

	var prunedNodes []*LinkedNode
	toDelete := make(map[Key]bool)
	for k := range t.keyValue {
		if _, keep := retained[k]; !keep {
			toDelete[k] = true
		}
	}

	for k := range toDelete {
		if nv, ok := t.keyValue[k]; ok && nv != nil {
			prunedNodes = append(prunedNodes, cloneLinkedNode(nv))
		}
		delete(t.headers, k)
		delete(t.keyValue, k)
		delete(t.parentToChild, k)
	}

	// Remove deleted nodes from parent-child maps.
	for parentKey := range t.parentToChild {
		for childKey := range t.parentToChild[parentKey] {
			if toDelete[childKey] {
				delete(t.parentToChild[parentKey], childKey)
			}
		}
		if len(t.parentToChild[parentKey]) == 0 {
			delete(t.parentToChild, parentKey)
		}
	}

	newRootValue := newRoot.Node
	t.root = &newRootValue
	prunedNodes = append(prunedNodes, t.pruneOrphansAtOrBelow(t.root.Height)...)

	return prunedNodes
}

func (t *BlockTree) selectPruneRoot(bestNV *LinkedNode, minHeight, maxHeight uint64) *LinkedNode {
	for current := bestNV; current != nil; {
		if current.Height <= maxHeight {
			if current.Height < minHeight {
				return nil
			}
			return current
		}
		if current.ParentKey == "" {
			return nil
		}
		parent, ok := t.keyValue[current.ParentKey]
		if !ok {
			return nil
		}
		current = parent
	}
	return nil
}

func (t *BlockTree) collectDescendants(key Key, retained map[Key]struct{}) {
	if _, seen := retained[key]; seen {
		return
	}
	if t.keyValue[key] == nil {
		return
	}

	retained[key] = struct{}{}
	for childKey := range t.parentToChild[key] {
		t.collectDescendants(childKey, retained)
	}
}

// pruneOrphansAtOrBelow removes orphan nodes whose height is less than or
// equal to threshold and returns them as LinkedNodes for unified prune output.
func (t *BlockTree) pruneOrphansAtOrBelow(threshold uint64) []*LinkedNode {
	var prunedOrphans []*LinkedNode

	for parentKey, siblings := range t.orphanParentToChild {
		for childKey, orphanNode := range siblings {
			if orphanNode == nil {
				delete(siblings, childKey)
				delete(t.orphanKeySet, childKey)
				continue
			}

			if orphanNode.Height <= threshold {
				prunedOrphans = append(prunedOrphans, &LinkedNode{Node: *orphanNode})
				delete(siblings, childKey)
				delete(t.orphanKeySet, childKey)
			}
		}

		if len(siblings) == 0 {
			delete(t.orphanParentToChild, parentKey)
		}
	}

	return prunedOrphans
}

// walk recursively removes the entire subtree rooted at key from the tree.
func walk(t *BlockTree, key Key) {
	for childKey := range t.parentToChild[key] {
		walk(t, childKey)
	}
	// Non-leaf nodes are not in headers; this delete is a safe no-op for them.
	delete(t.headers, key)
	delete(t.keyValue, key)
	delete(t.parentToChild, key)
}

// Root returns a copy of the current root, or nil if the tree is empty.
func (t *BlockTree) Root() *Node {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.root == nil {
		return nil
	}
	v := *t.root
	return &v
}

// Get returns the LinkedNode for key, or nil if not present.
func (t *BlockTree) Get(key Key) *LinkedNode {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return cloneLinkedNode(t.keyValue[key])
}

// HeightRange returns the current linked tree height range.
// start is the root height, end is the maximum height among headers.
// ok is false when the tree is empty.
func (t *BlockTree) HeightRange() (start, end uint64, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.root == nil || len(t.keyValue) == 0 {
		return 0, 0, false
	}

	start = t.root.Height
	end = start
	for k := range t.headers {
		nv := t.keyValue[k]
		if nv != nil && nv.Height > end {
			end = nv.Height
		}
	}
	return start, end, true
}

// UnlinkedNodes returns each orphan bucket's parent key: the key that orphan
// blocks are waiting for. Only parents that are not themselves buffered as
// orphans (not in orphanKeySet) are included.
func (t *BlockTree) UnlinkedNodes() []Key {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []Key
	for parentKey, children := range t.orphanParentToChild {
		if len(children) == 0 {
			continue
		}
		if _, ok := t.orphanKeySet[parentKey]; ok {
			continue
		}
		result = append(result, parentKey)
	}
	return result
}

// LinkedNodes returns all linked nodes currently present in keyValue.
func (t *BlockTree) LinkedNodes() []*LinkedNode {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*LinkedNode, 0, len(t.keyValue))
	for _, nv := range t.keyValue {
		if nv != nil {
			result = append(result, cloneLinkedNode(nv))
		}
	}
	return result
}

// Branches returns all branches by walking backward from every header.
//
// Branch ordering: header Height descending, then header Weight descending,
// then header Key ascending for deterministic output.
//
// Node ordering within a branch: leaf (header) first, then parent, …, toward root
// (same order as the backward walk; heights decrease along the slice).
func (t *BlockTree) Branches() []Branch {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.headers) == 0 {
		return nil
	}

	headerList := make([]*LinkedNode, 0, len(t.headers))
	for k := range t.headers {
		nv := t.keyValue[k]
		if nv != nil {
			headerList = append(headerList, cloneLinkedNode(nv))
		}
	}

	sort.Slice(headerList, func(i, j int) bool {
		if headerList[i].Height != headerList[j].Height {
			return headerList[i].Height > headerList[j].Height
		}
		if headerList[i].Weight != headerList[j].Weight {
			return headerList[i].Weight > headerList[j].Weight
		}
		return headerList[i].Key < headerList[j].Key
	})

	result := make([]Branch, 0, len(headerList))
	for _, header := range headerList {
		nodes := make([]*LinkedNode, 0)
		for cur := header; cur != nil; {
			nodes = append(nodes, cloneLinkedNode(cur))
			parent, ok := t.keyValue[cur.ParentKey]
			if !ok {
				break
			}
			cur = cloneLinkedNode(parent)
		}

		result = append(result, Branch{Header: cloneLinkedNode(header), Nodes: nodes})
	}

	return result
}

// Len returns the number of blocks in the tree (excluding orphans).
func (t *BlockTree) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.keyValue)
}
